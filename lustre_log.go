package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/spf13/viper"
)

type LustreEvent struct {
	Number int
	Type   string
	Target string
	Parent string
}

var c = cache.New(5*time.Minute, 10*time.Minute)

func listenLog() {
	eventsChan := make(chan string, viper.GetInt("listen_queue"))

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

	for j := 0; j < viper.GetInt("listen_workers"); j++ {
		go func() {
			for evtStr := range eventsChan {
				evtTokens := strings.Split(evtStr, " ")
				switch evtTokens[1] {
				case "01CREAT":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of created file: %s", err.Error())
					}
					LLFilesCreatedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				case "02MKDIR":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of created folder: %s", err.Error())
					}
					LLFoldersCreatedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				case "07RMDIR":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of deleted folder: %s", err.Error())
					}
					LLFoldersRemovedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				case "06UNLNK":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of deleted file: %s", err.Error())
					}
					LLFilesRemovedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				case "10OPEN":
					if len(evtTokens) < 5 || len(evtTokens[6]) < 5 {
						logger.Errorf("Got unusual open event: |%s| %d %d %s", evtTokens, len(evtTokens), len(evtTokens[6]), evtTokens[6])
					} else {
						fstype, group, user, err := getOwner(evtTokens[6][3 : len(evtTokens[6])-1])
						if err != nil {
							logger.Errorf("Error getting fid of opened file: %s", err.Error())
						}
						LLFilesOpenedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
					}
				case "14SATTR":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of changed attr %s: %s", evtTokens[5][3:len(evtTokens[5])-1], err.Error())
					}
					LLAttrChangedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				case "15XATTR":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of changed attr %s: %s", evtTokens[5][3:len(evtTokens[5])-1], err.Error())
					}
					LLXAttrChangedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				case "17MTIME":
					fstype, group, user, err := getOwner(evtTokens[5][3 : len(evtTokens[5])-1])
					if err != nil {
						logger.Errorf("Error getting fid of mtime %s: %s", evtTokens[5][3:len(evtTokens[5])-1], err.Error())
					}
					LLMtimeChangedCounter.WithLabelValues(fstype, group, user, *fsListenParam).Inc()
				default:
					logger.Errorf("Got unknown event: %s", evtTokens)
				}
			}
		}()
	}

}

func getOwner(fid string) (fstype string, project string, user string, err error) { // works for combined /projects and /scratch

	var rel string

	if relVal, found := c.Get(fid); found {
		rel = relVal.(string)
		LLCacheHitsCounter.WithLabelValues(*fsListenParam).Inc()
	} else {
		LLCacheMissesCounter.WithLabelValues(*fsListenParam).Inc()
		fs := dataBackends[*fsListenParam]

		pathB, err := exec.Command("lfs", "fid2path", fs.GetMountPath(), fid).Output()
		if err != nil {
			return "", "unknown", "unknown", err
		}

		path := string(pathB)
		rel, err = filepath.Rel(fs.GetMountPath(), path)
		if err != nil {
			return "", "unknown", "unknown", err
		}
		c.Set(fid, rel, cache.DefaultExpiration)
	}

	if len(strings.Split(rel, "/")) < 3 {
		return "", "", "", fmt.Errorf("Path %s does not contain group and user", rel)
	}

	splitPath := strings.Split(rel, "/")
	return splitPath[0], splitPath[1], splitPath[2], nil

}
