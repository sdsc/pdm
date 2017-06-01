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

func (l LustreDatastore) Open(filePath string) (io.ReadCloser, error) {
	return os.Open(path.Join(l.mountPath, filePath))
}

func (l LustreDatastore) Create(filePath string) (io.WriteCloser, error) {
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
	//log.Debugf("#goroutines: %d\n", runtime.NumGoroutine())
	outchan := make(chan []string)

	cmdName := "lfs"
	cmdArgs := []string{"find", path.Join(l.mountPath, dirPath), "-maxdepth", "1", "!", "-type", "d"}
	if !listFiles {
		cmdArgs = []string{"find", path.Join(l.mountPath, dirPath), "-maxdepth", "1", "-type", "d"}
	}

	//log.Debugf("Scanning %s, for files: %v", dirPath, listFiles)
	cmd := exec.Command(cmdName, cmdArgs...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Error running lustre find: %v", err)
		return nil, err
	}

	scanner := bufio.NewScanner(cmdReader)

	go func(outchan chan []string) {
		defer close(outchan)
		if !listFiles {
			for scanner.Scan() {
				folder := scanner.Text()
				//log.Debugf("Found folder in %s: %s", dirPath, folder)
				rel, err := filepath.Rel(l.mountPath, folder)
				if err != nil {
					log.Errorf("Error resolving folder %s: %v", folder, err)
					continue
				}
				if rel != "." && rel != dirPath {
					sendList := []string{rel}
					outchan <- sendList
				}
			}
		} else {
			var filesBuf []string
			for scanner.Scan() {
				if len(filesBuf) == FILE_CHUNKS {
					//log.Debugf("Found %d files in %s", len(filesBuf), dirPath)
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
				//log.Debugf("Found %d files in %s", len(filesBuf), dirPath)
				outchan <- filesBuf
			}
		}
	}(outchan)

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	return outchan, nil
}
