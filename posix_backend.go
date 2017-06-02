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
	shouldMount    bool
	canWrite       bool
	skipFilesNewer int
	skipFilesOlder int
}

func (l PosixDatastore) GetId() string {
	return l.id
}

func (l PosixDatastore) GetSkipFilesNewer() int {
	return l.skipFilesNewer
}

func (l PosixDatastore) GetSkipFilesOlder() int {
	return l.skipFilesOlder
}

func (l PosixDatastore) GetMetadata(filePath string) (os.FileInfo, error) {
	return os.Lstat(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Remove(filePath string) error {
	return os.Remove(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Open(filePath string) (io.ReadCloser, error) {
	return os.Open(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Create(filePath string) (io.WriteCloser, error) {
	return os.Create(path.Join(l.mountPath, filePath))
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

	cmdName := "find"
	cmdArgs := []string{path.Join(l.mountPath, dirPath), "-mindepth", "1", "-maxdepth", "1", "!", "-type", "d"}
	if !listFiles {
		cmdArgs = []string{path.Join(l.mountPath, dirPath), "-mindepth", "1", "-maxdepth", "1", "-type", "d"}
	}

	cmd := exec.Command(cmdName, cmdArgs...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmdReader)
	go func() {
		defer close(outchan)
		if !listFiles {
			for scanner.Scan() {
				folder := scanner.Text()
				rel, err := filepath.Rel(l.mountPath, folder)
				if err != nil {
					log.Printf("Error resolving folder %s: %v", folder, err)
					continue
				}
				outchan <- []string{rel}
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
					log.Printf("Error resolving file %s: %v", file, err)
					continue
				}

				filesBuf = append(filesBuf, rel)
			}
			if len(filesBuf) > 0 {
				outchan <- filesBuf
			}
		}
	}()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	return outchan, nil
}
