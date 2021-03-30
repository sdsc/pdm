package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"gopkg.in/alecthomas/kingpin.v2"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"gopkg.in/sohlich/elogrus.v7"

	"math/rand"
)

type storage_backend interface {
	GetId() string
	GetMountPath() string
	GetSkipFilesNewer() int
	GetSkipFilesOlder() int
	GetPurgeFilesOlder() int
	GetPurgeDryRun() bool
	GetLocalFilepath(filePath string) string
	GetPriority() uint8
	GetSkipPaths() []string
	GetElasticIndex() string
	IsRecogniseTypes() bool
	IsNoGroup() bool
	IsUseCtimePurge() bool
	GetMetadata(filePath string) (os.FileInfo, error)
	Readlink(filePath string) (string, error)
	Symlink(pointTo, filePath string) error
	Remove(filePath string) error
	RemoveAll(filePath string) error
	Open(filePath string) (io.ReadCloser, error)
	Create(filePath string, meta os.FileInfo) (io.WriteCloser, error)
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

	viper.SetDefault("file_prefetch", 256)
	viper.SetDefault("dir_prefetch", 36)

	viper.SetDefault("dir_workers", 4)
	viper.SetDefault("file_workers", 16)
	viper.SetDefault("monitor_mount_sec", 2)
	viper.SetDefault("elastic_index", "idx")

	viper.SetDefault("copy", "true")
	viper.SetDefault("scan", "true")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

// These are variables used by workers to keep the statistics and periodically send
// these via rabbitmq to the aggregator
var (
	FilesCopiedCount   uint64
	FilesRemovedCount  uint64
	FilesSkippedCount  uint64
	FilesIndexedCount  uint64
	BytesCount         uint64
	FoldersCopiedCount uint64
)

var logger = logrus.New()

// FileChunks is the maximum number of files to put in one message
const FileChunks = 1000

const prometheusTopic = "prometheus"

const tasksExchange = "tasks"

var dataBackends = make(map[string]storage_backend)

var pubChan = make(chan message, 512)

var elasticClient *elastic.Client

var ctx, done = context.WithCancel(context.Background())

type message struct {
	Body       []byte
	RoutingKey string
	Priority   uint8
}

type task struct {
	// One of: "copy", "clear", "scan"
	Action   string
	ItemPath []string
}

type fileIdx struct {
	Path      string    `json:"path"`
	User      string    `json:"user"`
	Group     string    `json:"group"`
	Size      int64     `json:"size"`
	AllocSize int64     `json:"alloc_size"`
	Type      string    `json:"type"`
	Mtime     time.Time `json:"mtime,omitempty"`
	Atime     time.Time `json:"atime,omitempty"`
}

type session struct {
	*amqp.Connection
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
				logger.Debug("shutting down session factory")
				return
			}

			conn, err := amqp.Dial(url)
			if err != nil {
				logger.Fatalf("cannot (re)dial: %v: %q", err, url)
			}

			select {
			case sess <- session{conn}:
			case <-ctx.Done():
				logger.Debug("shutting down new session")
				return
			}
		}
	}()

	return sessions
}

func publish(sessions chan chan session, messages <-chan message, cancel context.CancelFunc) {

	for session := range sessions {
		logger.Info("redial publish")
		var (
			running bool
			reading = messages
			pending = make(chan message, 1)
			confirm = make(chan amqp.Confirmation, 1)
		)

		pub := <-session

		ch, err := pub.Channel()
		if err != nil {
			logger.Fatalf("cannot create channel: %v", err)
			continue
		}

		if err := ch.ExchangeDeclare(tasksExchange, "topic", false, true, false, false, nil); err != nil {
			logger.Fatalf("cannot declare exchange: %v", err)
			continue
		}

		// publisher confirms for this channel/connection
		if err := ch.Confirm(false); err != nil {
			logger.Error("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			ch.NotifyPublish(confirm)
		}

		logger.Debug("publishing...")

	Publish:
		for {
			var msg message
			select {
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}
				if !confirmed.Ack {
					logger.Debugf("nack message %d, body: %q", confirmed.DeliveryTag, string(msg.Body))
				}
				reading = messages

			case msg = <-pending:
				curExchange := tasksExchange
				if msg.RoutingKey == prometheusTopic {
					curExchange = "amq.topic"
				}
				err := ch.Publish(curExchange, msg.RoutingKey, false, false, amqp.Publishing{
					Body:         msg.Body,
					DeliveryMode: amqp.Persistent,
					Priority:     msg.Priority,
				})
				// Retry failed delivery on the next session
				if err != nil {
					pending <- msg
					ch.Close()
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
		logger.Info("redial subscribe")
		sub := <-session

		filech, err := sub.Channel()
		if err != nil {
			logger.Fatalf("cannot create channel: %v", err)
			continue
		}

		err = filech.Qos(viper.GetInt("file_prefetch"), 0, false)
		if err != nil {
			logger.Fatalf("cannot set channel QoS: %v", err)
		}

		dirch, err := sub.Channel()
		if err != nil {
			logger.Fatalf("cannot create channel: %v", err)
			continue
		}

		err = dirch.Qos(viper.GetInt("dir_prefetch"), 0, false)
		if err != nil {
			logger.Fatalf("cannot set channel QoS: %v", err)
		}

		var wg sync.WaitGroup

		if viper.GetBool("copy") {
			for k := range viper.Get("datasource").(map[string]interface{}) {
				if viper.GetBool(fmt.Sprintf("datasource.%s.write", k)) {
					for k2 := range viper.Get("datasource").(map[string]interface{}) {
						if k2 != k {
							routingKeyFile, routingKeyDir := fmt.Sprintf("file.%s.%s", k2, k), fmt.Sprintf("dir.%s.%s", k2, k)

							queueFile, err := filech.QueueDeclare(routingKeyFile, false, false, false, false, amqp.Table{"x-queue-mode": "lazy", "x-max-priority": 9})
							if err != nil {
								logger.Errorf("cannot consume from exclusive queue: %q, %v", queueFile, err)
								return
							}

							if err := filech.QueueBind(queueFile.Name, routingKeyFile, tasksExchange, false, nil); err != nil {
								logger.Errorf("cannot consume without a binding to exchange: %q, %v", tasksExchange, err)
								return
							}

							deliveriesFile, err := filech.Consume(queueFile.Name, "", false, false, false, false, nil)
							if err != nil {
								logger.Errorf("cannot consume from: %q, %v", queueFile, err)
								return
							}

							queueDir, err := dirch.QueueDeclare(routingKeyDir, false, false, false, false, amqp.Table{"x-queue-mode": "lazy"})
							if err != nil {
								logger.Errorf("cannot consume from exclusive queue: %q, %v", queueDir, err)
								return
							}

							if err := dirch.QueueBind(queueDir.Name, routingKeyDir, tasksExchange, false, nil); err != nil {
								logger.Errorf("cannot consume without a binding to exchange: %q, %v", tasksExchange, err)
								return
							}

							deliveriesDir, err := dirch.Consume(queueDir.Name, "", false, false, false, false, nil)
							if err != nil {
								logger.Errorf("cannot consume from: %q, %v", queueDir, err)
								return
							}

							wg.Add(2)

							go func() {
								defer wg.Done()
								for msg := range deliveriesFile {
									file_messages <- msg
								}
							}()
							go func() {
								defer wg.Done()
								for msg := range deliveriesDir {
									folder_messages <- msg
								}
							}()
						}
					}
				}
			}
		}

		if viper.GetBool("scan") {
			for k := range viper.Get("datasource").(map[string]interface{}) {
				routingKeyFile, routingKeyDir := fmt.Sprintf("file.%s", k), fmt.Sprintf("dir.%s", k)

				queueFile, err := filech.QueueDeclare(routingKeyFile, false, false, false, false, amqp.Table{"x-queue-mode": "lazy", "x-max-priority": 10})
				if err != nil {
					logger.Errorf("cannot consume from exclusive queue: %q, %v", queueFile, err)
					return
				}

				if err := filech.QueueBind(queueFile.Name, routingKeyFile, tasksExchange, false, nil); err != nil {
					logger.Errorf("cannot consume without a binding to exchange: %q, %v", tasksExchange, err)
					return
				}

				deliveriesFile, err := filech.Consume(queueFile.Name, "", false, false, false, false, nil)
				if err != nil {
					logger.Errorf("cannot consume from: %q, %v", queueFile, err)
					return
				}

				queueDir, err := dirch.QueueDeclare(routingKeyDir, false, false, false, false, amqp.Table{"x-queue-mode": "lazy"})
				if err != nil {
					logger.Errorf("cannot consume from exclusive queue: %q, %v", queueDir, err)
					return
				}

				if err := dirch.QueueBind(queueDir.Name, routingKeyDir, tasksExchange, false, nil); err != nil {
					logger.Errorf("cannot consume without a binding to exchange: %q, %v", tasksExchange, err)
					return
				}

				deliveriesDir, err := dirch.Consume(queueDir.Name, "", false, false, false, false, nil)
				if err != nil {
					logger.Errorf("cannot consume from: %q, %v", queueDir, err)
					return
				}

				wg.Add(2)

				go func() {
					defer wg.Done()
					for msg := range deliveriesFile {
						file_messages <- msg
					}
				}()
				go func() {
					defer wg.Done()
					for msg := range deliveriesDir {
						folder_messages <- msg
					}
				}()
			}
		}

		wg.Wait()
	}
}

func initElasticLog() {
	hostname, err := os.Hostname()
	if err != nil {
		logger.Error("Error getting hostname: ", err)
		return
	}

	elasticClient_, err := elastic.NewClient(elastic.SetURL(viper.GetString("elastic_url")))
	elasticClient = elasticClient_
	if err != nil {
		logger.Panic(err)
		return
	}

	hook, err := elogrus.NewElasticHook(elasticClient, hostname, logrus.DebugLevel, "pdmlog")
	if err != nil {
		logger.Panic(err)
		return
	}
	logger.Hooks.Add(hook)

	logger.Out = ioutil.Discard

	for _, dataBackend := range dataBackends {
		if exists, err := elasticClient.IndexExists(dataBackend.GetElasticIndex()).Do(context.Background()); err != nil {
			logger.Error(err)
		} else {
			if !exists {
				_, err = elasticClient.CreateIndex(dataBackend.GetElasticIndex()).BodyString(mapping).Do(context.Background())
				logger.Info("Created index %s", dataBackend.GetElasticIndex())
				if err != nil {
					logger.Error(err)
				}
			}
		}
	}

	logger.Debug("Initing the elastic log done")

}

var (
	app = kingpin.New("pdm", "Parallel data mover.")

	worker = app.Command("worker", "Run a worker")

	copyCommand             = app.Command("copy", "Copy a folder or a file")
	rabbitmqServerCopyParam = copyCommand.Flag("rabbitmq", "RabbitMQ connect string. (Can also be set in PDM_RABBITMQ environmental variable)").String()
	isFileCopyParam         = copyCommand.Flag("file", "Copy a file.").Bool()
	sourceCopyParam         = copyCommand.Arg("source", "The source mount ID").Required().String()
	targetCopyParam         = copyCommand.Arg("target", "The target mount ID").Required().String()
	pathCopyParam           = copyCommand.Arg("path", "The path to copy, relative to the mount").Required().String()

	clearCommand             = app.Command("clear", "Clear a folder or a file on target according to source")
	rabbitmqServerClearParam = clearCommand.Flag("rabbitmq", "RabbitMQ connect string.  (Can also be set in PDM_RABBITMQ environmental variable)").String()
	isFileClearParam         = clearCommand.Flag("file", "Clear a file.").Bool()
	sourceClearParam         = clearCommand.Arg("source", "The source mount ID").Required().String()
	targetClearParam         = clearCommand.Arg("target", "The target mount ID").Required().String()
	pathClearParam           = clearCommand.Arg("path", "The path to clear, relative to the mount").Required().String()

	scanCommand             = app.Command("scan", "Scan a folder or a file")
	rabbitmqServerScanParam = scanCommand.Flag("rabbitmq", "RabbitMQ connect string.  (Can also be set in PDM_RABBITMQ environmental variable)").String()
	isFileScanParam         = scanCommand.Flag("file", "Scan a file.").Bool()
	fsScanParam             = scanCommand.Arg("fs", "The fs mount ID").Required().String()
	pathScanParam           = scanCommand.Arg("path", "The path to scan, relative to the mount").Required().String()

	clearScanCommand             = app.Command("clearscan", "Scan the indexed files and remove the ones not existing")
	rabbitmqServerClearScanParam = clearScanCommand.Flag("rabbitmq", "RabbitMQ connect string.  (Can also be set in PDM_RABBITMQ environmental variable)").String()
	fsClearScanParam             = clearScanCommand.Arg("fs", "The fs mount ID").Required().String()

	listenLogCommand = app.Command("listen", "Listen to the lustre log")
	fsListenParam    = listenLogCommand.Arg("fs", "The fs mount ID").Required().String()
	portParam        = listenLogCommand.Arg("port", "The metrics prometheus port").Required().Int()
	listenMdtParam   = listenLogCommand.Arg("mdt", "The MDT ID and user in the form lustre-MDT0000:cl1").Required().Strings()

	monitor          = app.Command("monitor", "Start monitoring daemon")
	monitorPortParam = monitor.Arg("port", "The metrics prometheus port").Required().Int()

	purgeCommand             = app.Command("purge", "Purge old files")
	rabbitmqServerPurgeParam = purgeCommand.Flag("rabbitmq", "RabbitMQ connect string.  (Can also be set in PDM_RABBITMQ environmental variable)").String()
	fsPurgeParam             = purgeCommand.Arg("fs", "The fs mount ID").Required().String()
	pathPurgeParam           = purgeCommand.Arg("path", "The path to scan, relative to the mount").Required().String()
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case worker.FullCommand():
		readWorkerConfig()

		var checkMountpoints []string

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)
		go func() {
			for sig := range c {
				fmt.Printf("Got %v signal. Terminating gracefully.\n", sig)
				done()
			}
		}()

		for k := range viper.Get("datasource").(map[string]interface{}) {
			switch datastore_type := viper.GetString(fmt.Sprintf("datasource.%s.type", k)); datastore_type {
			case "lustre":
				dataBackends[k] = LustreDatastore{
					k,
					viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.write", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_newer_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_older_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.purge_files_older_days", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.mds", k)),
					viper.GetString(fmt.Sprintf("datasource.%s.elastic_index", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.recognise_types", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.no_group", k)),
					viper.GetStringSlice(fmt.Sprintf("datasource.%s.skip_path", k)),
					uint8(viper.GetInt(fmt.Sprintf("datasource.%s.priority", k))),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_dry_run", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_use_ctime", k)),
				}
			case "posix":
				dataBackends[k] = PosixDatastore{
					k,
					viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.write", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_newer_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_older_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.purge_files_older_days", k)),
					viper.GetString(fmt.Sprintf("datasource.%s.elastic_index", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.recognise_types", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.no_group", k)),
					viper.GetStringSlice(fmt.Sprintf("datasource.%s.skip_path", k)),
					uint8(viper.GetInt(fmt.Sprintf("datasource.%s.priority", k))),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_dry_run", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_use_ctime", k)),
				}
			}
			if viper.IsSet(fmt.Sprintf("datasource.%s.mount", k)) && viper.GetBool(fmt.Sprintf("datasource.%s.mount", k)) {
				checkMountpoints = append(checkMountpoints, viper.GetString(fmt.Sprintf("datasource.%s.path", k)))
			}
		}

		if viper.IsSet("elastic_url") {
			initElasticLog()
		}

		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		if len(checkMountpoints) > 0 {
			go func() {
				for range time.NewTicker(time.Duration(viper.GetInt("monitor_mount_sec")) * time.Second).C {
					var stat1, stat2 syscall.Stat_t
					for _, mountpoint := range checkMountpoints {
						if err := syscall.Stat(mountpoint, &stat1); err != nil {
							logger.Fatalf("Error checking mountpoint %s: %s", mountpoint, err)
						}
						if err := syscall.Stat("/", &stat2); err != nil {
							logger.Fatalf("Error checking mountpoint /: %s", err)
						}
						if stat1.Dev == stat2.Dev {
							logger.Fatalf("Filesystem %s is not mounted. Exiting", mountpoint)
							done()
						}
					}
				}
			}()
		}

		var workersWg sync.WaitGroup

		go func() {
			workersWg.Add(1)
			publish(redial(ctx, viper.GetString("rabbitmq.connect_string")), pubChan, nil)
			workersWg.Done()
			done()
		}()

		go func() {
			subscribe(redial(ctx, viper.GetString("rabbitmq.connect_string")), processFilesStream(&workersWg), processFoldersStream(&workersWg))
			done()
		}()

		go func() {
			for range time.NewTicker(time.Duration(viper.GetInt("monitor_interval")) * time.Second).C {
				curFilesCopiedCount := atomic.SwapUint64(&FilesCopiedCount, 0)
				curFilesRemovedCount := atomic.SwapUint64(&FilesRemovedCount, 0)
				curFilesSkippedCount := atomic.SwapUint64(&FilesSkippedCount, 0)
				curFilesIndexedCount := atomic.SwapUint64(&FilesIndexedCount, 0)
				curBytesCount := atomic.SwapUint64(&BytesCount, 0)
				curFoldersCopiedCount := atomic.SwapUint64(&FoldersCopiedCount, 0)
				hostname, err := os.Hostname()
				if err != nil {
					logger.Error("Error getting hostname: ", err)
				}
				msgBody := monMessage{
					"none",
					hostname,
					float64(curFilesCopiedCount),
					float64(curFilesRemovedCount),
					float64(curFilesSkippedCount),
					float64(curFilesIndexedCount),
					float64(curBytesCount),
					float64(curFoldersCopiedCount)}

				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				err = enc.Encode(msgBody)
				if err != nil {
					logger.Error("Error encoding message: ", err)
					continue
				}

				msg := message{buf.Bytes(), prometheusTopic, 0}
				pubChan <- msg

			}
		}()

		logger.Info("Worker started")

		workersWg.Wait()

	case copyCommand.FullCommand():
		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		rabbitmqServer := ""

		if os.Getenv("PDM_RABBITMQ") != "" {
			rabbitmqServer = os.Getenv("PDM_RABBITMQ")
		} else if *rabbitmqServerCopyParam != "" {
			rabbitmqServer = *rabbitmqServerCopyParam
		}

		pub_chan := make(chan message)

		go func() {
			publish(redial(ctx, rabbitmqServer), pub_chan, done)
		}()

		queuePrefix := "dir"
		if *isFileCopyParam {
			queuePrefix = "file"
		}

		msgTask := task{
			"copy",
			[]string{*pathCopyParam}}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(msgTask)
		if err != nil {
			logger.Error("Error encoding message: ", err)
			return
		}

		var msg = message{buf.Bytes(), queuePrefix + "." + *sourceCopyParam + "." + *targetCopyParam, 0}
		pub_chan <- msg
		close(pub_chan)

	case clearCommand.FullCommand():
		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		rabbitmqServer := ""

		if os.Getenv("PDM_RABBITMQ") != "" {
			rabbitmqServer = os.Getenv("PDM_RABBITMQ")
		} else if *rabbitmqServerClearParam != "" {
			rabbitmqServer = *rabbitmqServerClearParam
		}

		pub_chan := make(chan message)

		go func() {
			publish(redial(ctx, rabbitmqServer), pub_chan, done)
		}()

		queuePrefix := "dir"
		if *isFileClearParam {
			queuePrefix = "file"
		}

		msgTask := task{
			"clear",
			[]string{*pathClearParam}}

		taskEnc, err := encodeTask(msgTask)
		if err != nil {
			logger.Error("Error encoding message: ", err)
			return
		}

		var msg = message{taskEnc, queuePrefix + "." + *sourceClearParam + "." + *targetClearParam, 0}
		pub_chan <- msg
		close(pub_chan)

	case scanCommand.FullCommand():
		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		rabbitmqServer := ""

		if os.Getenv("PDM_RABBITMQ") != "" {
			rabbitmqServer = os.Getenv("PDM_RABBITMQ")
		} else if *rabbitmqServerScanParam != "" {
			rabbitmqServer = *rabbitmqServerScanParam
		}

		pub_chan := make(chan message)

		go func() {
			publish(redial(ctx, rabbitmqServer), pub_chan, done)
		}()

		queuePrefix := "dir"
		if *isFileScanParam {
			queuePrefix = "file"
		}

		msgTask := task{
			"scan",
			[]string{*pathScanParam}}

		taskEnc, err := encodeTask(msgTask)
		if err != nil {
			logger.Error("Error encoding message: ", err)
			return
		}

		var msg = message{taskEnc, queuePrefix + "." + *fsScanParam, 0}
		pub_chan <- msg
		close(pub_chan)

	case purgeCommand.FullCommand():
		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		rabbitmqServer := ""

		if os.Getenv("PDM_RABBITMQ") != "" {
			rabbitmqServer = os.Getenv("PDM_RABBITMQ")
		} else if *rabbitmqServerScanParam != "" {
			rabbitmqServer = *rabbitmqServerPurgeParam
		}

		pub_chan := make(chan message)

		go func() {
			publish(redial(ctx, rabbitmqServer), pub_chan, done)
		}()

		queuePrefix := "dir"

		msgTask := task{
			"purge",
			[]string{*pathPurgeParam}}

		taskEnc, err := encodeTask(msgTask)
		if err != nil {
			logger.Error("Error encoding message: ", err)
			return
		}

		var msg = message{taskEnc, queuePrefix + "." + *fsPurgeParam, 0}
		pub_chan <- msg
		close(pub_chan)

	case clearScanCommand.FullCommand():
		readWorkerConfig()

		if viper.IsSet("elastic_url") {
			initElasticLog()
		}

		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		for k := range viper.Get("datasource").(map[string]interface{}) {
			switch datastore_type := viper.GetString(fmt.Sprintf("datasource.%s.type", k)); datastore_type {
			case "lustre":
				dataBackends[k] = LustreDatastore{
					k,
					viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.write", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_newer_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_older_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.purge_files_older_days", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.mds", k)),
					viper.GetString(fmt.Sprintf("datasource.%s.elastic_index", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.recognise_types", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.no_group", k)),
					viper.GetStringSlice(fmt.Sprintf("datasource.%s.skip_path", k)),
					uint8(viper.GetInt(fmt.Sprintf("datasource.%s.no_group", k))),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_dry_run", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_use_ctime", k)),
				}
			}
		}

		var workersWg sync.WaitGroup

		go func() {
			workersWg.Add(1)
			publish(redial(ctx, viper.GetString("rabbitmq.connect_string")), pubChan, nil)
			workersWg.Done()
			done()
		}()

		workersWg.Wait()

		getElasticFiles(dataBackends[*fsClearScanParam])

	case monitor.FullCommand():
		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}

		readWorkerConfig()
		prometheus.MustRegister(FilesCopiedCounter)
		prometheus.MustRegister(FilesRemovedCounter)
		prometheus.MustRegister(FilesSkippedCounter)
		prometheus.MustRegister(FilesIndexedCounter)
		prometheus.MustRegister(BytesCounter)
		prometheus.MustRegister(FoldersCopiedCounter)
		prometheus.MustRegister(QueueMessagesGauge)

		go func() {
			subscribeMon(redial(ctx, viper.GetString("rabbitmq.connect_string")), processMonitorStream(), prometheusTopic)
			done()
		}()

		http.Handle("/metrics", promhttp.Handler())
		logger.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *monitorPortParam), nil))

	case listenLogCommand.FullCommand():
		readWorkerConfig()
		if viper.IsSet("elastic_url") {
			initElasticLog()
		}

		if viper.IsSet("debug") && viper.GetBool("debug") {
			logger.Level = logrus.DebugLevel
		}
		for k := range viper.Get("datasource").(map[string]interface{}) {
			switch datastore_type := viper.GetString(fmt.Sprintf("datasource.%s.type", k)); datastore_type {
			case "lustre":
				dataBackends[k] = LustreDatastore{
					k,
					viper.GetString(fmt.Sprintf("datasource.%s.path", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.write", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_newer_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.skip_files_older_minutes", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.purge_files_older_days", k)),
					viper.GetInt(fmt.Sprintf("datasource.%s.mds", k)),
					viper.GetString(fmt.Sprintf("datasource.%s.elastic_index", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.recognise_types", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.no_group", k)),
					viper.GetStringSlice(fmt.Sprintf("datasource.%s.skip_path", k)),
					uint8(viper.GetInt(fmt.Sprintf("datasource.%s.no_group", k))),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_dry_run", k)),
					viper.GetBool(fmt.Sprintf("datasource.%s.purge_use_ctime", k)),
				}
			}
		}

		listenLog()
		prometheus.MustRegister(LLFilesCreatedCounter)
		prometheus.MustRegister(LLFilesRemovedCounter)
		prometheus.MustRegister(LLFoldersCreatedCounter)
		prometheus.MustRegister(LLFoldersRemovedCounter)
		prometheus.MustRegister(LLAttrChangedCounter)
		prometheus.MustRegister(LLXAttrChangedCounter)
		prometheus.MustRegister(LLMtimeChangedCounter)
		prometheus.MustRegister(LLCacheHitsCounter)
		prometheus.MustRegister(LLCacheMissesCounter)
		prometheus.MustRegister(LLQueueLengthGauge)

		http.Handle("/metrics", promhttp.Handler())
		logger.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *portParam), nil))
	}

	<-ctx.Done()
}
