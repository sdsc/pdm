package main

import (
	"errors"
	"github.com/spf13/viper"
	"os/exec"
	"strings"
)

const mapping = `
{
    "mappings": {
      "file": {
        "properties": {
          "size": {
            "type": "long"
          },
          "type": {
			"type": "keyword"
          },
          "mtime": {
            "type": "date"
          },
          "atime": {
            "type": "date"
          }
        }
      }
    },
    "settings": {
        "number_of_shards": "5",
        "number_of_replicas": "0"
    }
}
`

func getFileType(path string) (string, error) {
	if !viper.IsSet("tika_jar") {
		return "", errors.New("Tika jar not set")
	}

	retType, err := exec.Command("java", "-jar", viper.GetString("tika_jar"), "-d", path).Output()
	retTypeStr := strings.TrimSpace(string(retType))
	return retTypeStr, err
}
