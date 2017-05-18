package main

import (
	"os"
)

type PosixDatastore struct {
	path  string
	mount bool
	write bool
}

func (l PosixDatastore) GetFileMetadata(filepath string) (os.FileInfo, error) {
	fi, err := os.Stat(filepath)

	if err != nil {
		return nil, err
	}
	return fi, nil
}
