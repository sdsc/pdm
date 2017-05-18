package main

import (
	"os"
)

type LustreDatastore struct {
	path string
	mount bool
	write bool
}

func (l LustreDatastore) GetFileMetadata(filepath string) (os.FileInfo, error)  {
	fi, err := os.Stat(filepath)
    if err != nil {
        return nil, err
    }
    return fi, nil
}
