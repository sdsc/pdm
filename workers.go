package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"crypto/md5"
	"fmt"

	"github.com/karalabe/bufioprop" //https://groups.google.com/forum/#!topic/golang-nuts/Mwn9buVnLmY
	"github.com/olivere/elastic/v7"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gopkg.in/cheggaaa/pb.v1"
)

func processFilesStream(wg *sync.WaitGroup) chan<- amqp.Delivery {
	msgs := make(chan amqp.Delivery)
	for i := 0; i < viper.GetInt("file_workers"); i++ {
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			for msg := range msgs {
				routingKeySplit := strings.Split(msg.RoutingKey, ".")
				fromDataStore := dataBackends[routingKeySplit[1]]
				var toDataStore storage_backend
				if len(routingKeySplit) > 2 {
					toDataStore = dataBackends[routingKeySplit[2]]
				}

				curTask, err := decodeTask(msg.Body)
				if err != nil {
					logger.Errorf("Error parsing message: %s", err)
					continue
				}

				processFiles(fromDataStore, toDataStore, curTask)
				msg.Acknowledger.Ack(msg.DeliveryTag, false)
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}(i)
	}
	return msgs
}

func processFoldersStream(wg *sync.WaitGroup) chan<- amqp.Delivery {
	msgs := make(chan amqp.Delivery)
	for i := 0; i < viper.GetInt("dir_workers"); i++ {
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			for msg := range msgs {
				routingKeySplit := strings.Split(msg.RoutingKey, ".")
				fromDataStore := dataBackends[routingKeySplit[1]]
				var toDataStore storage_backend
				if len(routingKeySplit) > 2 {
					toDataStore = dataBackends[routingKeySplit[2]]
				}

				curTask, err := decodeTask(msg.Body)
				if err != nil {
					logger.Errorf("Error parsing message: %s", err)
					continue
				}

				err = processFolder(fromDataStore, toDataStore, curTask)
				if err != nil {
					msg.Acknowledger.Nack(msg.DeliveryTag, false, true)
				} else {
					msg.Acknowledger.Ack(msg.DeliveryTag, false)
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}(i)
	}
	return msgs
}

func processFiles(fromDataStore storage_backend, toDataStore storage_backend, taskStruct task) {

	for _, ignorePath := range fromDataStore.GetSkipPaths() {
		if strings.HasPrefix(taskStruct.ItemPath[0], ignorePath) {
			logger.Errorf("Ignoring the configured ignore path %s", taskStruct.ItemPath[0])
			return
		}
	}

	switch taskStruct.Action {
	case "copy":
		var indexFiles []string
		if viper.GetBool("scan_update") {
			defer func() {
				if len(indexFiles) == 0 {
					return
				}
				msgTask := task{
					"scan",
					indexFiles}

				taskEnc, err := encodeTask(msgTask)
				if err != nil {
					logger.Error("Error encoding files message: ", err)
					return
				}

				msg := message{taskEnc, "file." + toDataStore.GetId(), fromDataStore.GetPriority()}
				pubChan <- msg
			}()
		}

		for _, filepath := range taskStruct.ItemPath {
			logger.Debugf("Processing %s", filepath)
			sourceFileMeta, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source file
					logger.Debugf("Error reading file %s metadata, not exists: %s", filepath, err)
				} else {
					logger.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
				continue
			}

			//if sourceFileMeta.Mode() & 2 != 0 {
			//	logger.Infof("World-writeable file %s", filepath)
			//}

			switch mode := sourceFileMeta.Mode(); {
			case mode.IsRegular():
				//TODO: check stripes

				sourceMtime := sourceFileMeta.ModTime()
				sourceStat := sourceFileMeta.Sys().(*syscall.Stat_t)
				sourceAtime := getAtime(sourceStat)

				if fromDataStore.GetSkipFilesNewer() > 0 && time.Since(sourceMtime).Minutes() < float64(fromDataStore.GetSkipFilesNewer()) {
					logger.Debugf("Skipping the file %s as too new", filepath)
					atomic.AddUint64(&FilesSkippedCount, 1)
					continue
				}

				if fromDataStore.GetSkipFilesOlder() > 0 && time.Since(sourceAtime).Minutes() > float64(fromDataStore.GetSkipFilesOlder()) {
					logger.Debugf("Skipping the file %s as too old", filepath)
					atomic.AddUint64(&FilesSkippedCount, 1)
					continue
				}

				if viper.GetBool("scan_update") {
					res, err := elasticClient.
						Exists().
						Index(fromDataStore.GetElasticIndex()).
						Id(fmt.Sprintf("%x", md5.Sum([]byte(filepath)))).
						Type("file").
						Do(context.Background())
					if err != nil {
						logger.Errorf("Error retrieving the document %s: %s", filepath, err.Error())
					} else {
						if !res {
							indexFiles = append(indexFiles, filepath)
							logger.Debugf("File %s is not in index")
						} else {
							res, err := elasticClient.
								Get().
								Index(fromDataStore.GetElasticIndex()).
								Id(fmt.Sprintf("%x", md5.Sum([]byte(filepath)))).
								Type("file").
								Do(context.Background())

							var f fileIdx
							if err = json.Unmarshal(res.Source, &f); err != nil {
								logger.Errorf("Error deserializing the document %s: %s", filepath, err.Error())
								indexFiles = append(indexFiles, filepath)
							} else {
								//logger.Debugf("File %s is in index", filepath)
								if f.Size != sourceStat.Size || f.Mtime != sourceMtime || f.AllocSize != sourceStat.Blocks*512 {
									logger.Debugf("File %s is %v, reindexing.", filepath, f)
									indexFiles = append(indexFiles, filepath)
								}
							}
						}
					}
				}

				if destFileMeta, err := toDataStore.GetMetadata(filepath); err == nil { // the dest file exists

					destMtime := destFileMeta.ModTime()

					if sourceFileMeta.Size() == destFileMeta.Size() &&
						sourceFileMeta.Mode() == destFileMeta.Mode() &&
						sourceMtime == destMtime {
						//logger.Debug("File %s hasn't been changed ", filepath)
						atomic.AddUint64(&FilesSkippedCount, 1)
						continue
					}
					//logger.Debugf("File %s exists and is modified ", filepath)
					err = toDataStore.Remove(filepath)
					if err != nil {
						logger.Error("Error removing file ", filepath, ": ", err)
						continue
					}
				}

				if viper.GetInt("skip_files_larger_gb") > 0 && sourceFileMeta.Size() > (1<<30)*viper.GetInt64("skip_files_larger_gb") {
					logger.Errorf("Skipping file > %d GB %s", viper.GetInt("skip_files_larger_gb"), filepath)
					continue
				}

				//logger.Debug("Started copying %s %d", filepath, worker)
				src, err := fromDataStore.Open(filepath)
				if err != nil {
					logger.Error("Error opening src file ", filepath, ": ", err)
					continue
				}
				dest, err := toDataStore.Create(filepath, sourceFileMeta)
				if err != nil {
					logger.Error("Error opening dst file ", filepath, ": ", err)
					continue
				}
				bytesCopied, err := bufioprop.Copy(dest, src, 1048559)
				if err != nil {
					logger.Error("Error copying file ", filepath, ": ", err)
					continue
				}

				src.Close()
				dest.Close()

				if err = toDataStore.Lchown(filepath, int(sourceFileMeta.Sys().(*syscall.Stat_t).Uid), int(sourceFileMeta.Sys().(*syscall.Stat_t).Gid)); err != nil {
					logger.Error(err.Error())
				}
				if err = toDataStore.Chmod(filepath, sourceFileMeta.Mode()); err != nil {
					logger.Error(err.Error())
				}
				if err = toDataStore.Chtimes(filepath, sourceAtime, sourceMtime); err != nil {
					logger.Error(err.Error())
				}

				atomic.AddUint64(&FilesCopiedCount, 1)
				atomic.AddUint64(&BytesCount, uint64(bytesCopied))

				//logger.Debug("Done copying %s: %d bytes", filepath, bytesCopied)
			case mode.IsDir():
				// shouldn't happen
				logger.Error("File ", filepath, " appeared to be a folder")
			case mode&os.ModeSymlink != 0:
				linkTarget, err := fromDataStore.Readlink(filepath)
				if err != nil {
					logger.Error("Error reading symlink ", filepath, ": ", err)
					continue
				}

				if _, err = toDataStore.GetMetadata(filepath); err == nil { // the dest link exists
					if dlinkTarget, err := toDataStore.Readlink(filepath); err == nil && dlinkTarget == linkTarget {
						atomic.AddUint64(&FilesCopiedCount, 1)
						continue
					}

					if err = toDataStore.Remove(filepath); err != nil {
						logger.Error("Error removing symlink ", filepath, ": ", err)
						continue
					}
				}

				if err = toDataStore.Symlink(linkTarget, filepath); err != nil {
					logger.Error("Error setting symlink ", filepath, ": ", err)
					continue
				}

				toDataStore.Lchown(filepath, int(sourceFileMeta.Sys().(*syscall.Stat_t).Uid), int(sourceFileMeta.Sys().(*syscall.Stat_t).Gid))

				atomic.AddUint64(&FilesCopiedCount, 1)

			case mode&os.ModeNamedPipe != 0:
				logger.Error("File ", filepath, " is a named pipe. Not supported yet.")
			}
		}
	case "clear":
		for _, filepath := range taskStruct.ItemPath {
			_, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) {
					logger.Debugf("Error reading file %s metadata, not exists, removing from target: %s", filepath, err)
					err = toDataStore.Remove(filepath)

					if viper.GetBool("scan_update") {
						elasticClient.
							Delete().
							Index(fromDataStore.GetElasticIndex()).
							Id(fmt.Sprintf("%x", md5.Sum([]byte(filepath)))).
							Type("file").
							Do(context.Background())
					}

					atomic.AddUint64(&FilesRemovedCount, 1)
					if err != nil {
						logger.Error("Error clearing target file ", filepath, ": ", err)
					}
				} else {
					logger.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
			} else { // else the source file exists
				atomic.AddUint64(&FilesSkippedCount, 1)
			}
		}
	case "purge":
		skipped := 0

		for _, filepath := range taskStruct.ItemPath {
			logger.Debugf("Processing %s", filepath)
			sourceFileMeta, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source file
					logger.Debugf("Error reading file %s metadata, not exists: %s", filepath, err)
				} else {
					logger.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
				continue
			}

			switch mode := sourceFileMeta.Mode(); {
			case mode.IsRegular():
				sourceStat := sourceFileMeta.Sys().(*syscall.Stat_t)
				sourceAtime := getAtime(sourceStat)
				if fromDataStore.IsUseCtimePurge() {
					sourceAtime = getCtime(sourceStat)
				}

				if fromDataStore.GetPurgeFilesOlder() > 0 && (time.Since(sourceAtime).Hours()/24.0 > float64(fromDataStore.GetPurgeFilesOlder())) {
					if !fromDataStore.GetPurgeDryRun() {
						logger.Infof("Deleting file %s", filepath)

						if !fromDataStore.IsPurgeToTrash() {
							if err := fromDataStore.Remove(filepath); err != nil {
								logger.Errorf("Error deleting file %s: %s", filepath, err.Error())
							}
						} else {
							if err := fromDataStore.Trash(filepath); err != nil {
								logger.Errorf("Error trashing file %s: %s", filepath, err.Error())
							}
						}
					} else {
						logger.Infof("Dry-run deleting file %s", filepath)
					}
					atomic.AddUint64(&FilesRemovedCount, 1)
				} else {
					atomic.AddUint64(&FilesSkippedCount, 1)
					skipped++
				}
			default:
				{
					skipped++
				}
			}

		}

		if skipped == 0 && len(taskStruct.ItemPath) > 0 { // dir is now empty
			if len(strings.Split(taskStruct.ItemPath[0], string(os.PathSeparator))) > 3 {
				if !fromDataStore.GetPurgeDryRun() {
					logger.Infof("Deleting dir %s", path.Dir(taskStruct.ItemPath[0]))
					if err := fromDataStore.Remove(path.Dir(taskStruct.ItemPath[0])); err != nil {
						logger.Errorf("Error deleting folder %s: %s", path.Dir(taskStruct.ItemPath[0]), err.Error())
					}
				} else {
					logger.Infof("Dry run deleting folder %s", path.Dir(taskStruct.ItemPath[0]))
				}
			}
		}
	case "scan":
		for _, filepath := range taskStruct.ItemPath {
			logger.Debugf("Scanning %s", filepath)
			sourceFileMeta, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) {
					logger.Debugf("Error reading file %s metadata, not exists: %s", filepath, err)
				} else {
					logger.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
				continue
			}

			if sourceFileMeta.Mode().IsRegular() {
				fileType := ""
				if fromDataStore.IsRecogniseTypes() {
					fileTypeCur, err := getFileType(fromDataStore.GetLocalFilepath(filepath))
					if err != nil {
						logger.Errorf("Error recognising %s metadata: %s", filepath, err)
						continue
					}
					fileType = fileTypeCur
				}

				sourceStat := sourceFileMeta.Sys().(*syscall.Stat_t)
				sourceAtime := getAtime(sourceStat)

				// logger.Debugf("Scanning file %s of size %d and type %s", filepath, sourceFileMeta.Size(), fileType)
				dirs := strings.Split(filepath, "/")
				var user, group string
				if fromDataStore.IsNoGroup() {
					if len(dirs) > 0 {
						user = dirs[0]
					}
				} else {
					if len(dirs) > 0 {
						group = dirs[0]
						if len(dirs) > 1 {
							user = dirs[1]
						}
					}
				}
				fileIndex := fileIdx{filepath, user, group, sourceFileMeta.Size(), sourceStat.Blocks * 512, fileType, sourceFileMeta.ModTime(), sourceAtime}
				_, err = elasticClient.Index().
					Index(fromDataStore.GetElasticIndex()).
					Id(fmt.Sprintf("%x", md5.Sum([]byte(filepath)))).
					Type("file").
					BodyJson(fileIndex).
					Do(context.Background())
				if err != nil {
					// Handle error
					logger.Errorf("Error adding file %s to index: %s", filepath, err)
				}
				logger.Debugf("%s is %s", filepath, fileType)
				atomic.AddUint64(&FilesIndexedCount, 1)
			}

		}
	case "clearscan":
		for _, filepath := range taskStruct.ItemPath {
			_, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) {
					logger.Debugf("Error reading file %s metadata, not exists, removing from index: %s", filepath, err)
					elasticClient.
						Delete().
						Index(fromDataStore.GetElasticIndex()).
						Id(fmt.Sprintf("%x", md5.Sum([]byte(filepath)))).
						Type("file").
						Do(context.Background())
					atomic.AddUint64(&FilesRemovedCount, 1)
					if err != nil {
						logger.Error("Error clearing target file ", filepath, ": ", err)
					}
				} else {
					logger.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
			} else { // else the source file exists
				atomic.AddUint64(&FilesSkippedCount, 1)
			}
		}
	}
}

func processFolder(fromDataStore storage_backend, toDataStore storage_backend, taskStruct task) error {
	dirPath := taskStruct.ItemPath[0]

	defer atomic.AddUint64(&FoldersCopiedCount, 1)

	for _, ignorePath := range fromDataStore.GetSkipPaths() {
		if strings.HasPrefix(dirPath, ignorePath) {
			logger.Debugf("Ignoring the configured ignore path %s", dirPath)
			return nil
		}
	}

	logger.Debugf("Processing folder %s", dirPath)
	switch taskStruct.Action {
	case "copy":
		logger.Debugf("Processing folder %s", dirPath)
		if dirPath != "/" {
			sourceDirMeta, err := fromDataStore.GetMetadata(dirPath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source folder
					logger.Debugf("Error reading folder %s metadata or source folder not exists: %s", dirPath, err)
					return nil
				} else {
					logger.Errorf("Error reading folder %s: %s", dirPath, err)
					return err
				}
			}

			if destDirMeta, err := toDataStore.GetMetadata(dirPath); err == nil { // the dest folder exists
				sourceDirStat := sourceDirMeta.Sys().(*syscall.Stat_t)
				sourceDirUid := int(sourceDirStat.Uid)
				sourceDirGid := int(sourceDirStat.Gid)

				destDirStat := destDirMeta.Sys().(*syscall.Stat_t)
				destDirUid := int(destDirStat.Uid)
				destDirGid := int(destDirStat.Gid)

				if destDirMeta.Mode() != sourceDirMeta.Mode() {
					toDataStore.Chmod(dirPath, sourceDirMeta.Mode())
				}

				if sourceDirUid != destDirUid || sourceDirGid != destDirGid {
					toDataStore.Lchown(dirPath, sourceDirUid, sourceDirGid)
				}

			} else {
				toDataStore.Mkdir(dirPath, sourceDirMeta.Mode())
				toDataStore.Lchown(dirPath, int(sourceDirMeta.Sys().(*syscall.Stat_t).Uid), int(sourceDirMeta.Sys().(*syscall.Stat_t).Gid))
			}
		}

		dirsChan, err := fromDataStore.ListDir(dirPath, false)
		if err != nil {
			logger.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		count := 0

		for dir := range dirsChan {
			count++
			msgTask := task{
				"copy",
				dir}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				logger.Error("Error encoding dir message: ", err)
				continue
			}

			msg := message{taskEnc, "dir." + fromDataStore.GetId() + "." + toDataStore.GetId(), fromDataStore.GetPriority()}
			pubChan <- msg
		}

		filesChan, err := fromDataStore.ListDir(dirPath, true)
		if err != nil {
			logger.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for files := range filesChan {
			count++
			msgTask := task{
				"copy",
				files}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				logger.Error("Error encoding monitoring message: ", err)
				continue
			}

			msg := message{taskEnc, "file." + fromDataStore.GetId() + "." + toDataStore.GetId(), fromDataStore.GetPriority()}
			pubChan <- msg
		}

		//if count == 0 {
		//	logger.Errorf("Folder %s is empty", dirPath)
		//}

	case "clear":
		if dirPath != "/" {
			_, err := fromDataStore.GetMetadata(dirPath)
			if err != nil {
				toDataStore.RemoveAll(dirPath)
				return nil
			}
		}

		dirsChan, err := toDataStore.ListDir(dirPath, false)
		if err != nil {
			logger.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for dir := range dirsChan {
			msgTask := task{
				"clear",
				dir}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				logger.Error("Error encoding dir message: ", err)
				continue
			}

			msg := message{taskEnc, "dir." + fromDataStore.GetId() + "." + toDataStore.GetId(), fromDataStore.GetPriority()}
			pubChan <- msg
		}

		filesChan, err := toDataStore.ListDir(dirPath, true)
		if err != nil {
			logger.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for files := range filesChan {
			msgTask := task{
				"clear",
				files}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				logger.Error("Error encoding monitoring message: ", err)
				continue
			}

			msg := message{taskEnc, "file." + fromDataStore.GetId() + "." + toDataStore.GetId(), fromDataStore.GetPriority()}
			pubChan <- msg
		}

	case "scan", "purge":

		var dirMeta fs.FileInfo

		if dirPath != "/" {
			var err error
			dirMeta, err = fromDataStore.GetMetadata(dirPath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source folder
					logger.Debugf("Source folder not exists: %s", dirPath, err)
					return nil
				} else {
					logger.Errorf("Error reading source folder %s: %s", dirPath, err)
					return err
				}
			}
		}

		dirsChan, err := fromDataStore.ListDir(dirPath, false)
		if err != nil {
			logger.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for dir := range dirsChan {
			msgTask := task{
				taskStruct.Action,
				dir}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				logger.Error("Error encoding dir message: ", err)
				continue
			}

			msg := message{taskEnc, "dir." + fromDataStore.GetId(), fromDataStore.GetPriority()}
			pubChan <- msg
		}

		filesChan, err := fromDataStore.ListDir(dirPath, true)
		if err != nil {
			logger.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		filesCount := 0

		for files := range filesChan {
			filesCount++
			msgTask := task{
				taskStruct.Action,
				files}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				logger.Error("Error encoding monitoring message: ", err)
				continue
			}

			msg := message{taskEnc, "file." + fromDataStore.GetId(), fromDataStore.GetPriority()}
			pubChan <- msg
		}

		if taskStruct.Action == "purge" && filesCount == 0 && len(strings.Split(dirPath, "/")) > 3 { // del empty folders, don't want to delete upper ones
			dirCtime := getCtime(dirMeta.Sys().(*syscall.Stat_t))
			if time.Since(dirCtime).Hours()/24 > float64(fromDataStore.GetPurgeFoldersOlder()) {
				if !fromDataStore.GetPurgeDryRun() {
					logger.Infof("Deleting folder %s", dirPath)
					fromDataStore.Remove(dirPath)
				} else {
					logger.Infof("Dry-run deleting folder %s", dirPath)
				}
			}
		}
	}

	return nil
}

func encodeTask(taskStruct task) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(taskStruct)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeTask(taskBytes []byte) (task, error) {
	var curTask task
	buf := bytes.NewBuffer(taskBytes)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&curTask)
	if err != nil {
		return curTask, err
	}
	return curTask, nil
}

func getElasticFiles(dataStore storage_backend) {
	client, err := elastic.NewClient(elastic.SetURL(viper.GetString("elastic_url")))
	if err != nil {
		logger.Panic(err)
		return
	}
	//Count total and setup progress
	total, err := client.Count(dataStore.GetElasticIndex()).Type("file").Do(context.Background())
	if err != nil {
		panic(err)
	}
	bar := pb.StartNew(int(total))

	hits := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(hits)
		scroll := client.
			Scroll(dataStore.GetElasticIndex()).
			Type("file").
			Size(1000).
			FetchSourceContext(elastic.NewFetchSourceContext(true).Include("path"))
		for {
			results, err := scroll.Do(context.Background())
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				select {
				case hits <- hit.Source:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})

	for i := 0; i < 10; i++ {
		g.Go(func() error {
			var filesBuf []string

			for hit := range hits {
				//var p fileIdx
				var p map[string]string
				err := json.Unmarshal(hit, &p)
				if err != nil {
					return err
				}

				filesBuf = append(filesBuf, p["path"])

				logger.Debug("Added %s", p["path"])

				if len(filesBuf) == FileChunks {
					msgTask := task{
						"clearscan",
						filesBuf}

					taskEnc, err := encodeTask(msgTask)
					if err != nil {
						logger.Error("Error encoding clearscan message: ", err)
						continue
					}

					msg := message{taskEnc, "file." + dataStore.GetId(), dataStore.GetPriority()}
					pubChan <- msg
					filesBuf = filesBuf[:0]
				}

				bar.Increment()

				// Terminate early?
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if len(filesBuf) > 0 {
				msgTask := task{
					"clearscan",
					filesBuf}

				taskEnc, err := encodeTask(msgTask)
				if err != nil {
					logger.Error("Error encoding clearscan message: ", err)
				}

				msg := message{taskEnc, "file." + dataStore.GetId(), dataStore.GetPriority()}
				pubChan <- msg
			}

			return nil
		})
	}

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		panic(err)
	}

	// Done.
	bar.FinishPrint("Done")

}
