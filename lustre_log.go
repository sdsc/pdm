package main

import (
	"bufio"
	"fmt"
	"github.com/patrickmn/go-cache"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type LustreEvent struct {
	Number int
	Type   string
	Target string
	Parent string
}

var c = cache.New(5*time.Minute, 10*time.Minute)

func listenLog() {
	eventsChan := make(chan string, 100000)

	go func() {
		for range time.NewTicker(5 * time.Second).C {
			LLQueueLengthGauge.WithLabelValues(*fsListenParam).Set(float64(len(eventsChan)))
		}
	}()

	for _, mdtstr := range *listenMdtParam {
		mdt := strings.Split(mdtstr, ":")[0]
		user := strings.Split(mdtstr, ":")[1]
		go func(mdt string, user string) {
			cmdName := "lfs"
			cmdArgs := []string{"changelog", mdt, user}

			for {

				cmd := exec.Command(cmdName, cmdArgs...)
				cmdReader, err := cmd.StdoutPipe()
				if err != nil {
					log.Println(err.Error())
				}

				stderr, err := cmd.StderrPipe()
				if err != nil {
					log.Println(err.Error())
				}

				scanner := bufio.NewScanner(cmdReader)

				err = cmd.Start()
				if err != nil {
					log.Println(err.Error())
				}

				var lastID string
				for scanner.Scan() {
					newEvt := scanner.Text()
					eventsChan <- newEvt
					lastID = strings.Split(newEvt, " ")[0]
				}

				slurp, _ := ioutil.ReadAll(stderr)

				if err = cmd.Wait(); err != nil {
					log.Printf("%s %s", err.Error(), slurp)
				}

				if lastID == "" {
					time.Sleep(1 * time.Second)
				} else {
					if _, err = exec.Command("lfs", "changelog_clear", mdt, user, lastID).Output(); err != nil {
						log.Println(err.Error())
					}
				}
			}
		}(mdt, user)
	}

	for i := 0; i < 2; i++ {
		go func() {
			for evtStr := range eventsChan {
				evtTokens := strings.Split(evtStr, " ")
				switch evtTokens[1] {
				case "01CREAT":
					group, user, err := getOwner(evtTokens[6][3 : len(evtTokens[6])-1])
					if err != nil {
						logger.Errorf("Error getting fid of created file: %s", err.Error())
					}
					LLFilesCreatedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				case "02MKDIR":
					group, user, err := getOwner(evtTokens[6][3 : len(evtTokens[6])-1])
					if err != nil {
						logger.Errorf("Error getting fid of created folder: %s", err.Error())
					}
					LLFoldersCreatedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				case "07RMDIR":
					group, user, err := getOwner(evtTokens[6][3 : len(evtTokens[6])-1])
					if err != nil {
						logger.Errorf("Error getting fid of deleted folder: %s", err.Error())
					}
					LLFoldersRemovedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				case "06UNLNK":
					group, user, err := getOwner(evtTokens[6][3 : len(evtTokens[6])-1])
					if err != nil {
						logger.Errorf("Error getting fid of deleted file: %s", err.Error())
					}
					LLFilesRemovedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				case "14SATTR":
					group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of changed attr %s: %s", evtTokens[5][3 : len(evtTokens[5])-1], err.Error())
					}
					LLAttrChangedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				case "15XATTR":
					group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of changed attr %s: %s", evtTokens[5][3 : len(evtTokens[5])-1], err.Error())
					}
					LLXAttrChangedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				case "17MTIME":
					group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of mtime %s: %s", evtTokens[5][3:len(evtTokens[5])-1], err.Error())
					}
					LLMtimeChangedCounter.WithLabelValues(group, user, *fsListenParam).Inc()
				default:
					logger.Errorf("Got unknown event: %s", evtTokens)
				}
			}
		}()
	}

}

func getOwner(fid string) (string, string, error) {

	var rel string

	if relVal, found := c.Get(fid); found {
		rel = relVal.(string)
		LLCacheHitsCounter.WithLabelValues(*fsListenParam).Inc()
	} else {
		LLCacheMissesCounter.WithLabelValues(*fsListenParam).Inc()
		fs := dataBackends[*fsListenParam]

		pathB, err := exec.Command("lfs", "fid2path", fs.GetMountPath(), fid).Output()
		if err != nil {
			return "unknown", "unknown", err
		}

		path := string(pathB)
		rel, err = filepath.Rel(fs.GetMountPath(), path)
		if err != nil {
			return "", "", err
		}
		c.Set(fid, rel, cache.DefaultExpiration)
	}

	if len(strings.Split(rel, "/")) < 2 {
		return "", "", fmt.Errorf("Path %s does not contain group and user", rel)
	}

	splitPath := strings.Split(rel, "/")
	return splitPath[0], splitPath[1], nil

}
