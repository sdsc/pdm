package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"time"
	//"github.com/intel-hpdd/go-lustre/llapi"
	"log"
	"path"
	"path/filepath"
)

type LustreDatastore struct {
	id          string
	mountPath   string
	shouldMount bool
	canWrite    bool
}

func (l LustreDatastore) GetId() string {
	return l.id
}

func (l LustreDatastore) GetMetadata(filePath string) (os.FileInfo, error) {
	return os.Stat(path.Join(l.mountPath, filePath))
}

//func (l LustreDatastore) GetSpecMetadata(filePath string) (map[string]interface{}, error) {
//	retMeta := make(map[string]interface{})
//	return
//}

func (l LustreDatastore) Remove(filePath string) error {
	return os.Remove(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Open(filePath string) (io.Reader, error) {
	return os.Open(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Create(filePath string) (io.Writer, error) {
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

	cmdName := "lfs"
	cmdArgs := []string{"find", path.Join(l.mountPath, dirPath), "-maxdepth", "1", "!", "-type", "d"}
	if !listFiles {
		cmdArgs = []string{"find", path.Join(l.mountPath, dirPath), "-maxdepth", "1", "-type", "d"}
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
				if rel != "." && rel != dirPath {
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
