package main

import (
	"os"
	"io"
	//"github.com/intel-hpdd/go-lustre/llapi"
	"path"
)

type LustreDatastore struct {
	mountPath   string
	shouldMount bool
	canWrite    bool
}

func (l LustreDatastore) GetFileMetadata(filePath string) (os.FileInfo, error) {
	// fid, err := Path2Fid(filepath)
	// if err != nil {
	// 	return nil, err
	// }

	return os.Stat(path.Join(l.mountPath, filePath))
}

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
