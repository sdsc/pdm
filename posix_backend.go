package main

import (
	"os"
	"io"
	"path"
)

type PosixDatastore struct {
	mountPath   string
	shouldMount bool
	canWrite    bool
}

func (l PosixDatastore) GetMetadata(filePath string) (os.FileInfo, error) {
	return os.Stat(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Remove(filePath string) error {
	return os.Remove(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Open(filePath string) (io.Reader, error) {
	return os.Open(path.Join(l.mountPath, filePath))
}

func (l PosixDatastore) Create(filePath string) (io.Writer, error) {
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
