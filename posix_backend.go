package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"
)

type PosixDatastore struct {
	id             string
	mountPath      string
	canWrite       bool
	skipFilesNewer int
	skipFilesOlder int
}

func (l PosixDatastore) GetId() string {
	return l.id
}

func (l PosixDatastore) GetMountPath() string {
	return l.mountPath
}

func (l PosixDatastore) GetSkipFilesNewer() int {
	return l.skipFilesNewer
}

func (l PosixDatastore) GetSkipFilesOlder() int {
	return l.skipFilesOlder
}

func (l PosixDatastore) GetLocalFilepath(filePath string) string {
	return path.Join(l.mountPath, filePath)
}

func (l PosixDatastore) GetMetadata(filePath string) (os.FileInfo, error) {
	return os.Lstat(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Readlink(filePath string) (string, error) {
	return os.Readlink(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Symlink(pointTo, filePath string) error {
	return os.Symlink(pointTo, path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Remove(filePath string) error {
	return os.Remove(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) RemoveAll(filePath string) error {
	return os.RemoveAll(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Open(filePath string) (io.ReadCloser, error) {
	return os.Open(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Create(filePath string, meta os.FileInfo) (io.WriteCloser, error) {
	return os.OpenFile(path.Join(l.mountPath, filePath), os.O_RDWR|os.O_TRUNC, meta.Mode())
}

func (l PosixDatastore) Lchown(filePath string, uid, gid int) error {
	return os.Lchown(path.Join(l.mountPath, filePath), uid, gid)
}

func (l PosixDatastore) Chmod(filePath string, perm os.FileMode) error {
	return os.Chmod(path.Join(l.mountPath, filePath), perm)
}

func (l PosixDatastore) Mkdir(dirPath string, perm os.FileMode) error {
	return os.Mkdir(path.Join(l.mountPath, dirPath), perm)
}

func (l PosixDatastore) Chtimes(dirPath string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(path.Join(l.mountPath, dirPath), atime, mtime)
}

func (l PosixDatastore) ListDir(dirPath string, listFiles bool) (chan []string, error) {
	outchan := make(chan []string)

	curDir := path.Join(l.mountPath, dirPath)

	cmdName := "find"
	cmdArgs := []string{curDir, "-mindepth", "1", "-maxdepth", "1", "!", "-type", "d"}
	if !listFiles {
		cmdArgs = []string{curDir, "-mindepth", "1", "-maxdepth", "1", "-type", "d"}
	}

	cmd := exec.Command(cmdName, cmdArgs...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		logger.Errorf("Error running find: %v", err)
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
		cmd.Wait()
	}(outchan)

	return outchan, nil
}
