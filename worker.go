package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"log"
	. "github.com/tj/go-debug"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"github.com/karalabe/bufioprop" //https://groups.google.com/forum/#!topic/golang-nuts/Mwn9buVnLmY
)

type storage_backend interface {
	GetFileMetadata(filepath string) (os.FileInfo, error)
	Remove(filePath string) error
	Open(filePath string) (io.Reader, error)
	Create(filePath string) (io.Writer, error)
	Lchown(filePath string, uid, gid int) error
	Chmod(filePath string, perm os.FileMode) error
}

func readConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath("$HOME/.pdm")
	viper.AddConfigPath(".")

	viper.SetDefault("dir_workers", 2)
	viper.SetDefault("file_workers", 2)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

const exchange = "tasks"

var debug = Debug("worker")

var data_backends = make(map[string]storage_backend)

type message struct {
	Body       []byte
	RoutingKey string
}

type task struct {
	Action   string   `json:"action"`
	ItemPath []string `json:"item_path"`
}

type session struct {
	*amqp.Connection
	*amqp.Channel
}

func (s session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

func redial(ctx context.Context, url string) chan chan session {
	sessions := make(chan chan session)

	go func() {
		sess := make(chan session)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("shutting down session factory")
				return
			}

			conn, err := amqp.Dial(url)
			if err != nil {
				log.Fatalf("cannot (re)dial: %v: %q", err, url)
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Fatalf("cannot create channel: %v", err)
			}

			if err := ch.ExchangeDeclare(exchange, "topic", false, true, false, false, nil); err != nil {
				log.Fatalf("cannot declare exchange: %v", err)
			}

			select {
			case sess <- session{conn, ch}:
			case <-ctx.Done():
				log.Println("shutting down new session")
				return
			}
		}
	}()

	return sessions
}

func publish(sessions chan chan session, messages <-chan message) {
	var (
		running bool
		reading = messages
		pending = make(chan message, 1)
		confirm = make(chan amqp.Confirmation, 1)
	)

	for session := range sessions {
		pub := <-session

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			pub.NotifyPublish(confirm)
		}

		log.Printf("publishing...")

	Publish:
		for {
			var msg message
			select {
			case confirmed := <-confirm:
				if !confirmed.Ack {
					log.Printf("nack message %d, body: %q", confirmed.DeliveryTag, string(msg.Body))
				}
				reading = messages

			case msg = <-pending:
				err := pub.Publish(exchange, msg.RoutingKey, false, false, amqp.Publishing{
					Body: msg.Body,
				})
				// Retry failed delivery on the next session
				if err != nil {
					pending <- msg
					pub.Close()
					break Publish
				}

			case msg, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- msg
				reading = nil
			}
		}
	}
}

func subscribe(sessions chan chan session, file_messages chan<- message, folder_messages chan<- message) {

	for session := range sessions {
		sub := <-session

		var wg sync.WaitGroup

		for k := range viper.Get("datasource").(map[string]interface{}) {
			if viper.GetBool(fmt.Sprintf("datasource.%s.write", k)) {
				for k2 := range viper.Get("datasource").(map[string]interface{}) {
					if k2 != k {
						routingKeyFile, routingKeyDir := fmt.Sprintf("file.%s.%s", k2, k), fmt.Sprintf("dir.%s.%s", k2, k)

						queueFile, err := sub.QueueDeclare("", false, true, true, false, nil)
						if err != nil {
							log.Printf("cannot consume from exclusive queue: %q, %v", queueFile, err)
							return
						}

						if err := sub.QueueBind(queueFile.Name, routingKeyFile, exchange, false, nil); err != nil {
							log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
							return
						}

						deliveriesFile, err := sub.Consume(queueFile.Name, "", false, true, false, false, nil)
						if err != nil {
							log.Printf("cannot consume from: %q, %v", queueFile, err)
							return
						}

						queueDir, err := sub.QueueDeclare("", false, true, true, false, nil)
						if err != nil {
							log.Printf("cannot consume from exclusive queue: %q, %v", queueDir, err)
							return
						}

						if err := sub.QueueBind(queueDir.Name, routingKeyDir, exchange, false, nil); err != nil {
							log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
							return
						}

						deliveriesDir, err := sub.Consume(queueDir.Name, "", false, true, false, false, nil)
						if err != nil {
							log.Printf("cannot consume from: %q, %v", queueDir, err)
							return
						}

						wg.Add(2)

						go func() {
							defer wg.Done()
							for msg := range deliveriesFile {
								var new_msg = message{msg.Body, msg.RoutingKey}
								file_messages <- new_msg
								sub.Ack(msg.DeliveryTag, false)
							}
						}()
						go func() {
							defer wg.Done()
							for msg := range deliveriesDir {
								var new_msg = message{msg.Body, msg.RoutingKey}
								folder_messages <- new_msg
								sub.Ack(msg.DeliveryTag, false)
							}
						}()
					}
				}
			}
		}
		wg.Wait()
	}
}

func read(r io.Reader) <-chan message {
	ret_chan := make(chan message)
	go func() {
		defer close(ret_chan)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			var msg message
			msg.Body = scan.Bytes()
			msg.RoutingKey = "file.home.home2"
			ret_chan <- msg
		}
	}()
	return ret_chan
}

func processFilesStream() chan<- message {
	msgs := make(chan message)
	for i := 0; i <= viper.GetInt("file_workers"); i++ {
		go func(i int) {
			for msg := range msgs {
				var cur_task task
				var fromDataStore = data_backends[strings.Split(msg.RoutingKey, ".")[1]]
				var toDataStore = data_backends[strings.Split(msg.RoutingKey, ".")[2]]
				err := json.Unmarshal(msg.Body, &cur_task)
				if err != nil {
					log.Printf("Error parsing message: %s from %s", msg.Body, msg.RoutingKey)
					continue
				}
				processFiles(fromDataStore, toDataStore, cur_task)
			}
		}(i)
	}
	return msgs
}

func processFoldersStream() chan<- message {
	msgs := make(chan message)
	for i := 0; i <= viper.GetInt("folder_workers"); i++ {
		go func(i int) {
			for msg := range msgs {
				var cur_task task
				err := json.Unmarshal(msg.Body, &cur_task)
				if err != nil {
					log.Printf("Error parsing message: %s", msg.Body)
					continue
				}
				log.Print("Folder Worker ", i, " ", cur_task)
			}
		}(i)
	}
	return msgs
}

func processFiles(fromDataStore storage_backend, toDataStore storage_backend, taskStruct task) {
	for _, filepath := range taskStruct.ItemPath {
		sourceFileMeta, err := fromDataStore.GetFileMetadata(filepath)
		if err != nil {
			log.Print("Error reading file metadata: ", err)
		}

		debug("For file %s got meta %#v", filepath, sourceFileMeta)

		//TODO: check date

		switch mode := sourceFileMeta.Mode(); {
		case mode.IsRegular():
			//TODO: check stripes

			if destFileMeta, err := toDataStore.GetFileMetadata(filepath); err == nil { // the dest file exists
				if sourceFileMeta.Size() == destFileMeta.Size() &&
					sourceFileMeta.ModTime() == destFileMeta.ModTime() &&
					sourceFileMeta.Mode() == destFileMeta.Mode() {
					debug("File ", filepath, " hasn't been changed")
					return
				}
				err = toDataStore.Remove(filepath)
				if err != nil {
					log.Print("Error removing file %s: %s", filepath, err)
					continue
				}

				// TODO: setstripe
			}

			src, err := fromDataStore.Open(filepath)
			if err != nil {
				log.Printf("Error opening src file %s: %s", filepath, err)
				continue
			}
			dest, err := toDataStore.Create(filepath)
			if err != nil {
				log.Printf("Error opening dst file %s: %s", filepath, err)
				continue
			}
			copiedData, err := bufioprop.Copy(dest, src, 1048559)
			if err != nil {
				log.Printf("Error copying file %s: %s", filepath, err)
				continue
			}

			// toDataStore.Lchown(filepath, mode&os.ModeSetuid, mode&os.ModeSetgid)
			// toDataStore.Chmod(filepath, syscallMode(perm))

			debug("Done copying %s: %d bytes", filepath, copiedData)

			//{"action":"copy", "item_path":["/filelock.py"]} 
			// {"action":"copy", "item_path":["/noc-x86_64-Debian-8.ova"]}


            // copied_data = self.bcopy(src, dst, blksize)
            // os.chmod(dst, srcstat.st_mode)
            // os.utime(dst, (srcstat.st_atime, srcstat.st_mtime))

		case mode.IsDir():
			// shouldn't happen
		case mode&os.ModeSymlink != 0:
			fmt.Println("symbolic link")
		case mode&os.ModeNamedPipe != 0:
			fmt.Println("named pipe")
		}
	}
}

func main() {
	readConfig()

	for k := range viper.Get("datasource").(map[string]interface{}) {
		switch datastore_type := viper.GetString(fmt.Sprintf("datasource.%s.type", k)); datastore_type {
		case "lustre":
			data_backends[k] = LustreDatastore{
				viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
				viper.GetBool(fmt.Sprintf("datasource.%s.mount", k)),
				viper.GetBool(fmt.Sprintf("datasource.%s.write", k))}
		case "posix":
			data_backends[k] = PosixDatastore{
				viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
				viper.GetBool(fmt.Sprintf("datasource.%s.mount", k)),
				viper.GetBool(fmt.Sprintf("datasource.%s.write", k))}
		}
	}
	log.Print(data_backends)

	ctx, done := context.WithCancel(context.Background())

	go func() {
		publish(redial(ctx, viper.GetString("rabbitmq.connect_string")), read(os.Stdin))
		done()
	}()

	go func() {
		subscribe(redial(ctx, viper.GetString("rabbitmq.connect_string")), processFilesStream(), processFoldersStream())
		done()
	}()

	<-ctx.Done()
}
