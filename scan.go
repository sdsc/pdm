package main

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const mapping = `
{
    "mappings": {
      "file": {
        "properties": {
          "path": {
            "type": "keyword"
          },
          "user": {
            "type": "keyword"
          },
          "group": {
            "type": "keyword"
          },
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
        "number_of_shards": "10",
        "number_of_replicas": "0"
    }
}
`

const maxTikaSize = 65536

var client = &http.Client{Transport: &http.Transport{
	MaxIdleConnsPerHost: 24,
}}

func sendFileToTika(filename string) (string, error) {
	inp, _ := os.Open(filename)

	request, _ := http.NewRequest("PUT", viper.GetString("tika_url"), &io.LimitedReader{R: inp, N: maxTikaSize})
	request.Header.Add("Content-Disposition", fmt.Sprintf("attachment; filename=%s", strings.Replace(filepath.Base(filename), ":", "", -1)))
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func getFileType(path string) (string, error) {
	if viper.IsSet("tika_jar") {
		retType, err := exec.Command("java", "-jar", viper.GetString("tika_jar"), "-d", path).Output()
		retTypeStr := strings.TrimSpace(string(retType))
		return retTypeStr, err
	}

	if viper.IsSet("tika_url") {
		contentType, err := sendFileToTika(path)
		retTypeStr := strings.TrimSpace(contentType)
		return retTypeStr, err
	}

	return "", errors.New("Tika settings not set")
}
