package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type LustreEvent struct {
	Number int
	Type   string
	Target string
	Parent string
}

func listenLog(wg *sync.WaitGroup) {
	eventsChan := make(chan string, 10000)

	for _, mdtstr := range *listenMdtParam {
		mdt := strings.Split(mdtstr, ":")[0]
		user := strings.Split(mdtstr, ":")[1]
		go func(mdt string, user string) {
			cmdName := "lfs"
			cmdArgs := []string{"changelog", "--follow", mdt, user}

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
				time.Sleep(5 * time.Second)
			} else {
				if _, err = exec.Command("lfs", "changelog_clear", mdt, user, lastID).Output(); err != nil {
					log.Println(err.Error())
				}
			}
		}(mdt, user)
	}

	for i := 0; i < 3; i++ {
		go func() {
			for evtStr := range eventsChan {
				evtTokens := strings.Split(evtStr, " ")
				switch evtTokens[1] {
				case "01CREAT":
					log.Printf("Got create event: %s", evtTokens)
				default:
					log.Printf("Got event: %s", evtTokens)
				}
			}
		}()
	}

}
