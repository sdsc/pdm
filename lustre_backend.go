package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"time"
	//"runtime"
	//"github.com/intel-hpdd/go-lustre/llapi"
	"path"
	"path/filepath"
)

const LustreNoStripeSize =   10*1000000000
const Lustre5StripeSize =   100*1000000000
const Lustre10StripeSize = 1000*1000000000

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
	return os.RemoveAll(path.Join(l.mountPath, filePath))
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
			log.Debugf("Striping %s across 5", filePath)
		} else if meta.Size() < Lustre10StripeSize {
			cmdArgs = []string{"setstripe", "-c", "10", path.Join(l.mountPath, filePath)}
			log.Debugf("Striping %s across 10", filePath)
		} else {
			cmdArgs = []string{"setstripe", "-c", "50", path.Join(l.mountPath, filePath)}
			log.Debugf("Striping %s across 50", filePath)
		}

		_, err := exec.Command(cmdName, cmdArgs...).Output()
		if err != nil {
			log.Errorf("Error creating striped file: %v", err)
			return nil, err
		}
		return os.OpenFile(path.Join(l.mountPath, filePath), os.O_RDWR|os.O_TRUNC, 0666)
	}
	return os.Create(path.Join(l.mountPath, filePath))
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

	cmd := exec.Command(cmdName, cmdArgs...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Error running lustre find: %v", err)
		return nil, err
	}

	scanner := bufio.NewScanner(cmdReader)

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	go func(outchan chan []string) {
		defer close(outchan)
		if !listFiles {
			for scanner.Scan() {
				folder := scanner.Text()
				if folder != curDir {
					rel, err := filepath.Rel(l.mountPath, folder)
					if err != nil {
						log.Errorf("Error resolving folder %s: %v", folder, err)
						continue
					}
					outchan <- []string{rel}
				}
			}
		} else {
			var filesBuf []string
			for scanner.Scan() {
				if len(filesBuf) == FILE_CHUNKS {
					outchan <- filesBuf
					filesBuf = nil
				}

				file := scanner.Text()
				rel, err := filepath.Rel(l.mountPath, file)
				if err != nil {
					log.Errorf("Error resolving file %s: %v", file, err)
					continue
				}

				filesBuf = append(filesBuf, rel)
			}
			if len(filesBuf) > 0 {
				outchan <- filesBuf
			}
		}
		cmd.Wait()
	}(outchan)

	return outchan, nil
}

