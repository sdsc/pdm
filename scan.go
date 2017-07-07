package main

import (
	"errors"
	"github.com/spf13/viper"
	"os/exec"
	"strings"
)

func getFileType(path string) (string, error) {
	if !viper.IsSet("tika_jar") {
		return "", errors.New("Tika jar not set")
	}

	retType, err := exec.Command("java", "-jar", viper.GetString("tika_jar"), "-d", path).Output()
	retTypeStr := strings.TrimSpace(string(retType))
	return retTypeStr, err
}
