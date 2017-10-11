package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
	"sync/atomic"
	//"runtime"
	//"github.com/intel-hpdd/go-lustre/llapi"
	"path"
	"path/filepath"
)

const LustreNoStripeSize = 10 * 1000000000
const Lustre5StripeSize = 100 * 1000000000
const Lustre10StripeSize = 1000 * 1000000000

type LustreDatastore struct {
	id             string
	mountPath      string
	canWrite       bool
	skipFilesNewer int
	skipFilesOlder int
}

func (l LustreDatastore) GetId() string {
	return l.id
}

func (l LustreDatastore) GetSkipFilesNewer() int {
	return l.skipFilesNewer
}

func (l LustreDatastore) GetSkipFilesOlder() int {
	return l.skipFilesOlder
}

func (l LustreDatastore) GetLocalFilepath(filePath string) string {
	return path.Join(l.mountPath, filePath)
}

func (l LustreDatastore) GetMetadata(filePath string) (os.FileInfo, error) {
	return os.Lstat(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Readlink(filePath string) (string, error) {
	return os.Readlink(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Symlink(pointTo, filePath string) error {
	return os.Symlink(pointTo, path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Remove(filePath string) error {
	return os.Remove(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) RemoveAll(filePath string) error {
	return rmDir(path.Join(l.mountPath, filePath))
}

func rmDir(absPath string) error {
	logger.Debugf("Deleting folder: %v", absPath)
	dirsChan := make(chan string, 1000001)
	filesChan := make(chan string, 1000001)

	var wg sync.WaitGroup

	cmdDirName := "lfs"
	cmdDirArgs := []string{"find", absPath, "-maxdepth", "1", "-type", "d"}

	cmdDir := exec.Command(cmdDirName, cmdDirArgs...)
	cmdDirReader, err := cmdDir.StdoutPipe()
	if err != nil {
		logger.Errorf("Error running lustre find: %v", err)
		return err
	}

	scannerDir := bufio.NewScanner(cmdDirReader)

	err = cmdDir.Start()
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer close(dirsChan)
		defer wg.Done()

		for scannerDir.Scan() {
			newDir := scannerDir.Text()
			if newDir != absPath {
				dirsChan <- newDir
			}
		}
		cmdDir.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for dir := range dirsChan {
			err := rmDir(dir)
			if err != nil {
				logger.Errorf("Error processing folder %s: %v", dir, err)
			}
		}
	}()

	cmdFileName := "lfs"
	cmdFileArgs := []string{"find", absPath, "-maxdepth", "1", "!", "-type", "d"}

	cmdFile := exec.Command(cmdFileName, cmdFileArgs...)
	cmdFileReader, err := cmdFile.StdoutPipe()
	if err != nil {
		logger.Errorf("Error running lustre find: %v", err)
		return err
	}

	scannerFile := bufio.NewScanner(cmdFileReader)

	err = cmdFile.Start()
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer close(filesChan)
		defer wg.Done()

		for scannerFile.Scan() {
			newFile := scannerFile.Text()
			filesChan <- newFile
		}
		cmdFile.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for file := range filesChan {
			os.Remove(file)
			atomic.AddUint64(&FilesRemovedCount, 1)
		}
	}()

	wg.Wait()

	os.Remove(absPath)

	logger.Debugf("Successfully deleted folder: %v", absPath)
	return nil
}

func (l LustreDatastore) Open(filePath string) (io.ReadCloser, error) {
	return os.Open(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Create(filePath string, meta os.FileInfo) (io.WriteCloser, error) {
	if meta.Size() > LustreNoStripeSize {
		cmdName := "/usr/bin/lfs"
		var cmdArgs []string
		if meta.Size() < Lustre5StripeSize {
			cmdArgs = []string{"setstripe", "-c", "5", path.Join(l.mountPath, filePath)}
			logger.Debugf("Striping %s across 5", filePath)
		} else if meta.Size() < Lustre10StripeSize {
			cmdArgs = []string{"setstripe", "-c", "10", path.Join(l.mountPath, filePath)}
			logger.Debugf("Striping %s across 10", filePath)
		} else {
			cmdArgs = []string{"setstripe", "-c", "50", path.Join(l.mountPath, filePath)}
			logger.Debugf("Striping %s across 50", filePath)
		}

		_, err := exec.Command(cmdName, cmdArgs...).Output()
		if err != nil {
			logger.Errorf("Error creating striped file: %v", err)
			return nil, err
		}
	}
	return os.OpenFile(path.Join(l.mountPath, filePath), os.O_CREATE|os.O_RDWR|os.O_TRUNC, meta.Mode())
}

func (l LustreDatastore) Lchown(filePath string, uid, gid int) error {
	return os.Lchown(path.Join(l.mountPath, filePath), uid, gid)
}

func (l LustreDatastore) Chmod(filePath string, perm os.FileMode) error {
	return os.Chmod(path.Join(l.mountPath, filePath), perm)
}

func (l LustreDatastore) Mkdir(dirPath string, perm os.FileMode) error {
	return os.Mkdir(path.Join(l.mountPath, dirPath), perm)
}

func (l LustreDatastore) Chtimes(dirPath string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(path.Join(l.mountPath, dirPath), atime, mtime)
}

func (l LustreDatastore) ListDir(dirPath string, listFiles bool) (chan []string, error) {
	outchan := make(chan []string)

	curDir := path.Join(l.mountPath, dirPath)

	cmdName := "/usr/bin/lfs"
	cmdArgs := []string{"find", curDir, "-maxdepth", "1", "!", "-type", "d"}
	if !listFiles {
		cmdArgs = []string{"find", curDir, "-maxdepth", "1", "-type", "d"}
	}

	var cmd *exec.Cmd
	var scanner *bufio.Scanner
	ran := false

	for !ran {
		cmd = exec.Command(cmdName, cmdArgs...)

		cmdReader, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		scanner = bufio.NewScanner(cmdReader)

		err = cmd.Start()
		if err != nil {
			cmd.Wait()
			logger.Debugf("Error running lustre find: %v", err)
			if err.Error() != "fork/exec /usr/bin/lfs: errno 513" {
				return nil, err
			}
		}
		ran = true
	}

	go func(outchan chan []string) {
		defer close(outchan)
		defer cmd.Wait()
		if !listFiles {
			for scanner.Scan() {
				folder := scanner.Text()
				if folder != curDir {
					rel, err := filepath.Rel(l.mountPath, folder)
					if err != nil {
						logger.Errorf("Error resolving folder %s: %v", folder, err)
						continue
					}
					outchan <- []string{rel}
				}
			}
		} else {
			var filesBuf []string
			for scanner.Scan() {
				if len(filesBuf) == FileChunks {
					outchan <- filesBuf
					filesBuf = nil
				}

				file := scanner.Text()
				rel, err := filepath.Rel(l.mountPath, file)
				if err != nil {
					logger.Errorf("Error resolving file %s: %v", file, err)
					continue
				}

				filesBuf = append(filesBuf, rel)
			}
			if len(filesBuf) > 0 {
				outchan <- filesBuf
			}
		}
	}(outchan)

	return outchan, nil
}
