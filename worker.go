package main

import (
	"fmt"
	. "github.com/tj/go-debug"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	"sync/atomic"
	"encoding/gob"
	"bytes"

	"github.com/karalabe/bufioprop" //https://groups.google.com/forum/#!topic/golang-nuts/Mwn9buVnLmY
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type storage_backend interface {
	GetId() string
	GetMetadata(filepath string) (os.FileInfo, error)
	Remove(filePath string) error
	Open(filePath string) (io.Reader, error)
	Create(filePath string) (io.Writer, error)
	Lchown(filePath string, uid, gid int) error
	Chmod(filePath string, perm os.FileMode) error
	Mkdir(dirPath string, perm os.FileMode) error
	Chtimes(dirPath string, atime time.Time, mtime time.Time) error
	ListDir(dirPath string, listFiles bool) (chan []string, error)
}

type monitoring_backend interface {
	UpdateFiles(skipped int, processed int, bytes int) error
	UpdateFolders(processed int) error
}

func readWorkerConfig() {
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

// These are variables used by workers to keep the statistics and periodically send 
// these via rabbitmq to the aggregator
var (
	FilesCopiedCount uint64 = 0
	FilesSkippedCount uint64 = 0
	BytesCount uint64 = 0
	FoldersCopiedCount uint64 = 0
)

const FILE_CHUNKS = 1000

const exchange = "tasks"

const prometheusTopic = "prometheus"

var debug = Debug("worker")

var data_backends = make(map[string]storage_backend)

var pubChan = make(chan message)

type message struct {
	Body       []byte
	RoutingKey string
}

type task struct {
	Action   string
	ItemPath []string
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

			err = ch.Qos(36, 0, true)
			if err != nil {
				log.Fatalf("cannot set channel QoS: %v", err)
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

func publish(sessions chan chan session, messages <-chan message, cancel context.CancelFunc) {
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
				curExchange := exchange
				if(msg.RoutingKey == prometheusTopic) {
					curExchange = "amq.topic"
				}
				err := pub.Publish(curExchange, msg.RoutingKey, false, false, amqp.Publishing{
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
					if cancel != nil {
						cancel()
					}
					return
				}
				// work on pending delivery until ack'd
				pending <- msg
				reading = nil
			}
		}
	}
}

func subscribe(sessions chan chan session, file_messages chan<- amqp.Delivery, folder_messages chan<- amqp.Delivery) {

	for session := range sessions {
		sub := <-session

		var wg sync.WaitGroup

		for k := range viper.Get("datasource").(map[string]interface{}) {
			if viper.GetBool(fmt.Sprintf("datasource.%s.write", k)) {
				for k2 := range viper.Get("datasource").(map[string]interface{}) {
					if k2 != k {
						routingKeyFile, routingKeyDir := fmt.Sprintf("file.%s.%s", k2, k), fmt.Sprintf("dir.%s.%s", k2, k)

						queueFile, err := sub.QueueDeclare(routingKeyFile, false, false, false, false, nil)
						if err != nil {
							log.Printf("cannot consume from exclusive queue: %q, %v", queueFile, err)
							return
						}

						if err := sub.QueueBind(queueFile.Name, routingKeyFile, exchange, false, nil); err != nil {
							log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
							return
						}

						deliveriesFile, err := sub.Consume(queueFile.Name, "", false, false, false, false, nil)
						if err != nil {
							log.Printf("cannot consume from: %q, %v", queueFile, err)
							return
						}

						queueDir, err := sub.QueueDeclare(routingKeyDir, false, false, false, false, nil)
						if err != nil {
							log.Printf("cannot consume from exclusive queue: %q, %v", queueDir, err)
							return
						}

						if err := sub.QueueBind(queueDir.Name, routingKeyDir, exchange, false, nil); err != nil {
							log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
							return
						}

						deliveriesDir, err := sub.Consume(queueDir.Name, "", false, false, false, false, nil)
						if err != nil {
							log.Printf("cannot consume from: %q, %v", queueDir, err)
							return
						}

						wg.Add(2)

						go func() {
							defer wg.Done()
							for msg := range deliveriesFile {
								file_messages <- msg
								// sub.Ack(msg.DeliveryTag, false)
							}
						}()
						go func() {
							defer wg.Done()
							for msg := range deliveriesDir {
								folder_messages <- msg
								// sub.Ack(msg.DeliveryTag, false)
							}
						}()
					}
				}
			}
		}
		wg.Wait()
	}
}

func processFilesStream() chan<- amqp.Delivery {
	msgs := make(chan amqp.Delivery)
	for i := 0; i <= viper.GetInt("file_workers"); i++ {
		go func(i int) {
			for msg := range msgs {
				var curTask task
				var fromDataStore = data_backends[strings.Split(msg.RoutingKey, ".")[1]]
				var toDataStore = data_backends[strings.Split(msg.RoutingKey, ".")[2]]

				buf := bytes.NewBuffer(msg.Body)
				dec := gob.NewDecoder(buf)
				err := dec.Decode(&curTask)
				if err != nil {
					log.Printf("Error parsing message: %s", err)
					continue
				}

				processFiles(fromDataStore, toDataStore, curTask)
				msg.Acknowledger.Ack(msg.DeliveryTag, false)
			}
		}(i)
	}
	return msgs
}

func processFoldersStream() chan<- amqp.Delivery {
	msgs := make(chan amqp.Delivery)
	for i := 0; i <= viper.GetInt("folder_workers"); i++ {
		go func(i int) {
			for msg := range msgs {
				var curTask task
				var fromDataStore = data_backends[strings.Split(msg.RoutingKey, ".")[1]]
				var toDataStore = data_backends[strings.Split(msg.RoutingKey, ".")[2]]

				buf := bytes.NewBuffer(msg.Body)
				dec := gob.NewDecoder(buf)
				err := dec.Decode(&curTask)
				if err != nil {
					log.Printf("Error parsing message: %s", err)
					continue
				}

				processFolder(fromDataStore, toDataStore, curTask)
				msg.Acknowledger.Ack(msg.DeliveryTag, false)
			}
		}(i)
	}
	return msgs
}

func processFiles(fromDataStore storage_backend, toDataStore storage_backend, taskStruct task) {
	for _, filepath := range taskStruct.ItemPath {
		sourceFileMeta, err := fromDataStore.GetMetadata(filepath)
		if err != nil {
			log.Print("Error reading file metadata: ", err)
			continue
		}

		//debug("For file %s got meta %#v", filepath, sourceFileMeta)

		//TODO: check date

		switch mode := sourceFileMeta.Mode(); {
		case mode.IsRegular():
			//TODO: check stripes

			sourceMtime := sourceFileMeta.ModTime()
			sourceStat := sourceFileMeta.Sys().(*syscall.Stat_t)
			sourceAtime := time.Unix(int64(sourceStat.Atimespec.Sec), int64(sourceStat.Atimespec.Nsec))
			// sourceCtime = time.Unix(int64(sourceStat.Ctim.Sec), int64(sourceStat.Ctim.Nsec))

			if destFileMeta, err := toDataStore.GetMetadata(filepath); err == nil { // the dest file exists

				destMtime := destFileMeta.ModTime()

				if sourceFileMeta.Size() == destFileMeta.Size() &&
					sourceFileMeta.Mode() == destFileMeta.Mode() &&
					sourceMtime == destMtime {
					debug("File %s hasn't been changed", filepath)
					atomic.AddUint64(&FilesSkippedCount, 1)
					continue
				}
				debug("Removing file %s", filepath)
				err = toDataStore.Remove(filepath)
				if err != nil {
					log.Print("Error removing file %s: %s", filepath, err)
					continue
				}

				// TODO: setstripe
			}

			defer atomic.AddUint64(&FilesCopiedCount, 1)

			//debug("Started copying %s %d", filepath, worker)
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
			bytesCopied, err := bufioprop.Copy(dest, src, 1048559)
			if err != nil {
				log.Printf("Error copying file %s: %s", filepath, err)
				continue
			}
			atomic.AddUint64(&BytesCount, uint64(bytesCopied))

			toDataStore.Lchown(filepath, int(sourceFileMeta.Sys().(*syscall.Stat_t).Uid), int(sourceFileMeta.Sys().(*syscall.Stat_t).Gid))
			toDataStore.Chmod(filepath, sourceFileMeta.Mode())
			toDataStore.Chtimes(filepath, sourceAtime, sourceMtime)

			//debug("Done copying %s: %d bytes", filepath, copiedData)
		case mode.IsDir():
			// shouldn't happen
		case mode&os.ModeSymlink != 0:
			fmt.Println("symbolic link")
		case mode&os.ModeNamedPipe != 0:
			fmt.Println("named pipe")
		}
	}
}

func processFolder(fromDataStore storage_backend, toDataStore storage_backend, taskStruct task) {
	dirPath := taskStruct.ItemPath[0]
	debug("Processing folder %s", dirPath)

	defer atomic.AddUint64(&FoldersCopiedCount, 1)

	if dirPath != "/" {
		sourceDirMeta, err := fromDataStore.GetMetadata(dirPath)
		if err != nil {
			log.Print("Error reading folder metadata or source folder not exists: ", err)
			return
		}

		if destDirMeta, err := toDataStore.GetMetadata(dirPath); err == nil { // the dest folder exists
			debug("Dest dir exists: %#v", destDirMeta)

			sourceDirStat := sourceDirMeta.Sys().(*syscall.Stat_t)
			sourceDirUid := int(sourceDirStat.Uid)
			sourceDirGid := int(sourceDirStat.Uid)

			destDirStat := destDirMeta.Sys().(*syscall.Stat_t)
			destDirUid := int(destDirStat.Uid)
			destDirGid := int(destDirStat.Uid)

			if destDirMeta.Mode() != sourceDirMeta.Mode() {
				debug("Set dir chmod")
				toDataStore.Chmod(dirPath, sourceDirMeta.Mode())
			}

			if sourceDirUid != destDirUid || sourceDirGid != destDirGid {
				toDataStore.Lchown(dirPath, sourceDirUid, sourceDirGid)
				debug("Set dir chown")
			}

		} else {
			toDataStore.Mkdir(dirPath, sourceDirMeta.Mode())
			toDataStore.Chmod(dirPath, sourceDirMeta.Mode())
			toDataStore.Lchown(dirPath, int(sourceDirMeta.Sys().(*syscall.Stat_t).Uid), int(sourceDirMeta.Sys().(*syscall.Stat_t).Gid))
		}
	}

	dirsChan, err := fromDataStore.ListDir(dirPath, false)
	if err != nil {
		log.Print("Error listing folder: ", err)
		return
	}

	for dir := range dirsChan {
		debug("Found folder %s", dir)
		msg := message{[]byte(`{"action":"copy", "item_path":["` + dir[0] + `"]}`), "dir." + fromDataStore.GetId() + "." + toDataStore.GetId()}
		pubChan <- msg
	}

	filesChan, err := fromDataStore.ListDir(dirPath, true)
	if err != nil {
		log.Print("Error listing folder: ", err)
		return
	}

	for files := range filesChan {
		//debug("Found file %s", files)

		msgTask := task{
			"copy",
			files}

	    var buf bytes.Buffer
	    enc := gob.NewEncoder(&buf)
	    err = enc.Encode(msgTask)
	    if err != nil {
			log.Print("Error encoding monitoring message: ", err)
	        continue
	    }

		msg := message{buf.Bytes(), "file." + fromDataStore.GetId() + "." + toDataStore.GetId()}
		pubChan <- msg
	}

}

var (
	app = kingpin.New("pdm", "Parallel data mover.")

	worker = app.Command("worker", "Run a worker")

	copy                = app.Command("copy", "Copy a folder or a file")
	rabbitmqServerParam = copy.Flag("rabbitmq", "RabbitMQ connect string.").String()
	isFileParam         = copy.Flag("file", "Copy a file.").Bool()
	sourceParam         = copy.Arg("source", "The source mount").Required().String()
	targetParam         = copy.Arg("target", "The target mount").Required().String()
	pathParam           = copy.Arg("path", "The path to copy").Required().String()

	monitor = app.Command("monitor", "Start monitoring daemon")
)

func main() {
	ctx, done := context.WithCancel(context.Background())

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case worker.FullCommand():
		readWorkerConfig()

		for k := range viper.Get("datasource").(map[string]interface{}) {
			switch datastore_type := viper.GetString(fmt.Sprintf("datasource.%s.type", k)); datastore_type {
			case "lustre":
				data_backends[k] = LustreDatastore{
					k,
					viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.mount", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.write", k))}
			case "posix":
				data_backends[k] = PosixDatastore{
					k,
					viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.mount", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.write", k))}
			}
		}

		go func() {
			publish(redial(ctx, viper.GetString("rabbitmq.connect_string")), pubChan, nil)
			done()
		}()

		go func() {
			subscribe(redial(ctx, viper.GetString("rabbitmq.connect_string")), processFilesStream(), processFoldersStream())
			done()
		}()

		go func() {
		    for range time.NewTicker(time.Duration(viper.GetInt("monitor_interval")) * time.Second).C {
				curFilesCopiedCount := atomic.SwapUint64(&FilesCopiedCount, 0)
				curFilesSkippedCount := atomic.SwapUint64(&FilesSkippedCount, 0)
				curBytesCount := atomic.SwapUint64(&BytesCount, 0)
				curFoldersCopiedCount := atomic.SwapUint64(&FoldersCopiedCount, 0)
				hostname, err := os.Hostname()
				if err != nil {
					log.Print("Error getting hostname: ", err)
				}
				msgBody := monMessage{
					"none",
					hostname,
					float64(curFilesCopiedCount),
					float64(curFilesSkippedCount),
					float64(curBytesCount),
					float64(curFoldersCopiedCount)}

			    var buf bytes.Buffer
			    enc := gob.NewEncoder(&buf)
			    err = enc.Encode(msgBody)
			    if err != nil {
					log.Print("Error encoding monitoring message: ", err)
			        continue
			    }

				msg := message{buf.Bytes(), prometheusTopic}
				pubChan <- msg
			    
			}
		}()

	case copy.FullCommand():
		rabbitmqServer := ""

		if os.Getenv("PDM_RABBITMQ") != "" {
			rabbitmqServer = os.Getenv("PDM_RABBITMQ")
		} else if *rabbitmqServerParam != "" {
			rabbitmqServer = *rabbitmqServerParam
		}

		pub_chan := make(chan message)

		go func() {
			publish(redial(ctx, rabbitmqServer), pub_chan, done)
		}()

		queuePrefix := "dir"
		if *isFileParam {
			queuePrefix = "file"
		}

		msgTask := task{
			"copy",
			[]string{*pathParam}}

	    var buf bytes.Buffer
	    enc := gob.NewEncoder(&buf)
	    err = enc.Encode(msgTask)
	    if err != nil {
			log.Print("Error encoding monitoring message: ", err)
	        continue
	    }

		var msg = message{buf.Bytes(), queuePrefix + "." + *sourceParam + "." + *targetParam}
		pub_chan <- msg
		close(pub_chan)

	case monitor.FullCommand():
		readWorkerConfig()
		prometheus.MustRegister(FilesCopiedCounter)
		prometheus.MustRegister(FilesSkippedCounter)
		prometheus.MustRegister(BytesCounter)
		prometheus.MustRegister(FoldersCopiedCounter)

		go func() {
			subscribeMon(redial(ctx, viper.GetString("rabbitmq.connect_string")), processMonitorStream(), prometheusTopic)
			done()
		}()

		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8082", nil))

	}

	<-ctx.Done()

}
