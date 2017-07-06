package main

import (
	"bytes"
	"encoding/gob"
	"github.com/karalabe/bufioprop" //https://groups.google.com/forum/#!topic/golang-nuts/Mwn9buVnLmY
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"os"
	"strings"
	"sync/atomic"
	"sync"
	"syscall"
	"time"
)

func processFilesStream(wg *sync.WaitGroup) chan<- amqp.Delivery {
	msgs := make(chan amqp.Delivery)
	for i := 0; i < viper.GetInt("file_workers"); i++ {
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			for msg := range msgs {
				routingKeySplit := strings.Split(msg.RoutingKey, ".")
				fromDataStore := data_backends[routingKeySplit[1]]
				var toDataStore storage_backend
				if len(routingKeySplit) > 2 {
 					toDataStore = data_backends[routingKeySplit[2]]
				}

				curTask, err := decodeTask(msg.Body)
				if err != nil {
					log.Errorf("Error parsing message: %s", err)
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
				fromDataStore := data_backends[routingKeySplit[1]]
				var toDataStore storage_backend
				if len(routingKeySplit) > 2 {
 					toDataStore = data_backends[routingKeySplit[2]]
				}

				curTask, err := decodeTask(msg.Body)
				if err != nil {
					log.Errorf("Error parsing message: %s", err)
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
	switch taskStruct.Action {
	case "copy":

		for _, filepath := range taskStruct.ItemPath {
			//log.Debugf("Processing %s", filepath)
			sourceFileMeta, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source file
					log.Debugf("Error reading file %s metadata, not exists: %s", filepath, err)
				} else {
					log.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
				continue
			}

			switch mode := sourceFileMeta.Mode(); {
			case mode.IsRegular():
				//TODO: check stripes

				sourceMtime := sourceFileMeta.ModTime()
				sourceStat := sourceFileMeta.Sys().(*syscall.Stat_t)
				sourceAtime := getAtime(sourceStat)

				if fromDataStore.GetSkipFilesNewer() > 0 && time.Since(sourceMtime).Minutes() < float64(fromDataStore.GetSkipFilesNewer()) {
					log.Debugf("Skipping the file %s as too new", filepath)
					atomic.AddUint64(&FilesSkippedCount, 1)
					continue
				}

				if fromDataStore.GetSkipFilesOlder() > 0 && time.Since(sourceAtime).Minutes() > float64(fromDataStore.GetSkipFilesOlder()) {
					log.Debugf("Skipping the file %s as too old", filepath)
					atomic.AddUint64(&FilesSkippedCount, 1)
					continue
				}

				if destFileMeta, err := toDataStore.GetMetadata(filepath); err == nil { // the dest file exists

					destMtime := destFileMeta.ModTime()

					if sourceFileMeta.Size() == destFileMeta.Size() &&
						sourceFileMeta.Mode() == destFileMeta.Mode() &&
						sourceMtime == destMtime {
						//log.Debug("File %s hasn't been changed ", filepath)
						atomic.AddUint64(&FilesSkippedCount, 1)
						continue
					}
					//log.Debugf("File %s exists and is modified ", filepath)
					err = toDataStore.Remove(filepath)
					if err != nil {
						log.Error("Error removing file ", filepath, ": ", err)
						continue
					}

					// TODO: setstripe
				}

				//log.Debug("Started copying %s %d", filepath, worker)
				src, err := fromDataStore.Open(filepath)
				if err != nil {
					log.Error("Error opening src file ", filepath, ": ", err)
					continue
				}
				dest, err := toDataStore.Create(filepath, sourceFileMeta)
				if err != nil {
					log.Error("Error opening dst file ", filepath, ": ", err)
					continue
				}
				bytesCopied, err := bufioprop.Copy(dest, src, 1048559)
				if err != nil {
					log.Error("Error copying file ", filepath, ": ", err)
					continue
				}

				src.Close()
				dest.Close()

				toDataStore.Lchown(filepath, int(sourceFileMeta.Sys().(*syscall.Stat_t).Uid), int(sourceFileMeta.Sys().(*syscall.Stat_t).Gid))
				toDataStore.Chmod(filepath, sourceFileMeta.Mode())
				toDataStore.Chtimes(filepath, sourceAtime, sourceMtime)

				atomic.AddUint64(&FilesCopiedCount, 1)
				atomic.AddUint64(&BytesCount, uint64(bytesCopied))

				//log.Debug("Done copying %s: %d bytes", filepath, bytesCopied)
			case mode.IsDir():
				// shouldn't happen
				log.Error("File ", filepath, " appeared to be a folder")
			case mode&os.ModeSymlink != 0:
				sourceMtime := sourceFileMeta.ModTime()
				sourceStat := sourceFileMeta.Sys().(*syscall.Stat_t)
				sourceAtime := getAtime(sourceStat)
				if destFileMeta, err := toDataStore.GetMetadata(filepath); err == nil { // the dest link exists
					destMtime := destFileMeta.ModTime()

					if sourceFileMeta.Mode() == destFileMeta.Mode() &&
						sourceMtime == destMtime {
						atomic.AddUint64(&FilesSkippedCount, 1)
						continue
					}
					//log.Debugf("Removing symlink %s", filepath)
					err = toDataStore.Remove(filepath)
					if err != nil {
						log.Error("Error removing symlink ", filepath, ": ", err)
						continue
					}
				}

				linkTarget, err := fromDataStore.Readlink(filepath)
				if err != nil {
					log.Error("Error reading symlink ", filepath, ": ", err)
					continue
				}

				err = toDataStore.Symlink(linkTarget, filepath)
				if err != nil {
					log.Error("Error seting symlink ", filepath, ": ", err)
					continue
				}

				toDataStore.Lchown(filepath, int(sourceFileMeta.Sys().(*syscall.Stat_t).Uid), int(sourceFileMeta.Sys().(*syscall.Stat_t).Gid))
				toDataStore.Chtimes(filepath, sourceAtime, sourceMtime)

				atomic.AddUint64(&FilesCopiedCount, 1)

			case mode&os.ModeNamedPipe != 0:
				log.Error("File ", filepath, " is a named pipe. Not supported yet.")
			}
		}
	case "clear":
		for _, filepath := range taskStruct.ItemPath {
			_, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) {
					log.Debugf("Error reading file %s metadata, not exists, removing from target: %s", filepath, err)
					err = toDataStore.Remove(filepath)
					if err != nil {
						log.Error("Error clearing target file ", filepath, ": ", err)
					}
				} else {
					log.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
			} // else the source file exists - do nothing
		}
	case "scan":
		for _, filepath := range taskStruct.ItemPath {
			sourceFileMeta, err := fromDataStore.GetMetadata(filepath)
			if err != nil {
				if os.IsNotExist(err) {
					log.Debugf("Error reading file %s metadata, not exists: %s", filepath, err)
				} else {
					log.Errorf("Error reading file %s metadata: %s", filepath, err)
				}
				continue
			}

			if sourceFileMeta.Mode().IsRegular() {
				fileType, err := getFileType(fromDataStore.GetLocalFilepath(filepath))
				if err != nil {
					log.Errorf("Error recognising %s metadata: %s", filepath, err)
					continue
				}

				// log.Debugf("Scanning file %s of size %d and type %s", filepath, sourceFileMeta.Size(), fileType)
				fileIndex := fileIdx{sourceFileMeta.Size(), fileType}
				_, err = elasticClient.Index().
				    Index(viper.GetString("elastic_index")).
				    Type("file").
				    Id(filepath).
				    BodyJson(fileIndex).
				    Refresh("true").
				    Do(context.Background())
				if err != nil {
				    // Handle error
				    log.Errorf("Error adding file %s to index: %s",filepath, err)
				}
			    atomic.AddUint64(&FilesIndexedCount, 1)
			}

		}
	}
}

func processFolder(fromDataStore storage_backend, toDataStore storage_backend, taskStruct task) error {
	dirPath := taskStruct.ItemPath[0]

	defer atomic.AddUint64(&FoldersCopiedCount, 1)

	switch taskStruct.Action {
	case "copy":

		if dirPath != "/" {
			sourceDirMeta, err := fromDataStore.GetMetadata(dirPath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source folder
					log.Debugf("Error reading folder %s metadata or source folder not exists: %s", dirPath, err)
					return nil
				} else {
					log.Errorf("Error reading folder %s: %s", dirPath, err)
					return err
				}
			}

			if destDirMeta, err := toDataStore.GetMetadata(dirPath); err == nil { // the dest folder exists
				sourceDirStat := sourceDirMeta.Sys().(*syscall.Stat_t)
				sourceDirUid := int(sourceDirStat.Uid)
				sourceDirGid := int(sourceDirStat.Uid)

				destDirStat := destDirMeta.Sys().(*syscall.Stat_t)
				destDirUid := int(destDirStat.Uid)
				destDirGid := int(destDirStat.Uid)

				if destDirMeta.Mode() != sourceDirMeta.Mode() {
					toDataStore.Chmod(dirPath, sourceDirMeta.Mode())
				}

				if sourceDirUid != destDirUid || sourceDirGid != destDirGid {
					toDataStore.Lchown(dirPath, sourceDirUid, sourceDirGid)
				}

			} else {
				toDataStore.Mkdir(dirPath, sourceDirMeta.Mode())
				toDataStore.Chmod(dirPath, sourceDirMeta.Mode())
				toDataStore.Lchown(dirPath, int(sourceDirMeta.Sys().(*syscall.Stat_t).Uid), int(sourceDirMeta.Sys().(*syscall.Stat_t).Gid))
			}
		}

		dirsChan, err := fromDataStore.ListDir(dirPath, false)
		if err != nil {
			log.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for dir := range dirsChan {
			msgTask := task{
				"copy",
				dir}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				log.Error("Error encoding dir message: ", err)
				continue
			}

			msg := message{taskEnc, "dir." + fromDataStore.GetId() + "." + toDataStore.GetId()}
			pubChan <- msg
		}

		filesChan, err := fromDataStore.ListDir(dirPath, true)
		if err != nil {
			log.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for files := range filesChan {
			msgTask := task{
				"copy",
				files}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				log.Error("Error encoding monitoring message: ", err)
				continue
			}

			msg := message{taskEnc, "file." + fromDataStore.GetId() + "." + toDataStore.GetId()}
			pubChan <- msg
		}

	case "clear":
		if dirPath != "/" {
			_, err := fromDataStore.GetMetadata(dirPath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source folder
					log.Debugf("Source folder not exists: %s", dirPath, err)
					toDataStore.RemoveAll(dirPath)
					return nil
				} else {
					log.Errorf("Error reading source folder %s: %s", dirPath, err)
					return err
				}
			}
		}

		dirsChan, err := toDataStore.ListDir(dirPath, false)
		if err != nil {
			log.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for dir := range dirsChan {
			msgTask := task{
				"clear",
				dir}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				log.Error("Error encoding dir message: ", err)
				continue
			}

			msg := message{taskEnc, "dir." + fromDataStore.GetId() + "." + toDataStore.GetId()}
			pubChan <- msg
		}

		filesChan, err := toDataStore.ListDir(dirPath, true)
		if err != nil {
			log.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for files := range filesChan {
			msgTask := task{
				"clear",
				files}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				log.Error("Error encoding monitoring message: ", err)
				continue
			}

			msg := message{taskEnc, "file." + fromDataStore.GetId() + "." + toDataStore.GetId()}
			pubChan <- msg
		}

	case "scan":
		if dirPath != "/" {
			_, err := fromDataStore.GetMetadata(dirPath)
			if err != nil {
				if os.IsNotExist(err) { // the user already removed the source folder
					log.Debugf("Source folder not exists: %s", dirPath, err)
					return nil
				} else {
					log.Errorf("Error reading source folder %s: %s", dirPath, err)
					return err
				}
			}
		}

		dirsChan, err := fromDataStore.ListDir(dirPath, false)
		if err != nil {
			log.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for dir := range dirsChan {
			msgTask := task{
				"scan",
				dir}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				log.Error("Error encoding dir message: ", err)
				continue
			}

			msg := message{taskEnc, "dir." + fromDataStore.GetId()}
			pubChan <- msg
		}

		filesChan, err := fromDataStore.ListDir(dirPath, true)
		if err != nil {
			log.Errorf("Error listing folder %s: %s", dirPath, err)
			return err
		}

		for files := range filesChan {
			msgTask := task{
				"scan",
				files}

			taskEnc, err := encodeTask(msgTask)
			if err != nil {
				log.Error("Error encoding monitoring message: ", err)
				continue
			}

			msg := message{taskEnc, "file." + fromDataStore.GetId()}
			pubChan <- msg
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
