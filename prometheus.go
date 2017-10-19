package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

// Variables for general copy statistics
type monMessage struct {
	DataSource       string
	Node             string
	FilesCopied      float64
	FilesRemoved     float64
	FilesSkipped     float64
	FilesIndexed     float64
	BytesTransferred float64
	FoldersCopied    float64
}

var (
	FilesCopiedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "files_copied_number",
		Help: "Number of files copied",
	},
		[]string{"node", "datasource"})
	FilesRemovedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "files_removed_number",
		Help: "Number of files removed",
	},
		[]string{"node", "datasource"})
	FilesSkippedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "files_skipped_number",
		Help: "Number of files skipped",
	},
		[]string{"node", "datasource"})
	FilesIndexedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "files_indexed_number",
		Help: "Number of files indexed",
	},
		[]string{"node", "datasource"})
	BytesCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bytes_copied",
		Help: "Number of bytes transferred",
	},
		[]string{"node", "datasource"})
	FoldersCopiedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "folders_copied_number",
		Help: "Number of folders copied",
	},
		[]string{"node", "datasource"})
	QueueMessagesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_messages",
		Help: "Number of messages in the queue",
	},
		[]string{"id", "type"})
)


// Variables for lustre log listener
var (
	LLFilesCreatedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ll_files_created_number",
		Help: "Number of files created",
	},
		[]string{"group", "user", "datasource"})
	LLFilesRemovedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ll_files_removed_number",
		Help: "Number of files removed",
	},
		[]string{"group", "user", "datasource"})
	LLFoldersCreatedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ll_folders_created_number",
		Help: "Number of folders created",
	},
		[]string{"group", "user", "datasource"})
	LLFoldersRemovedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ll_folders_removed_number",
		Help: "Number of folders removed",
	},
		[]string{"group", "user", "datasource"})
	LLAttrChangedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ll_attr_changed_number",
		Help: "Number of files with attributes changed",
	},
		[]string{"group", "user", "datasource"})
	LLMtimeChangedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ll_mtime_changed_number",
		Help: "Number of files with mtime changed",
	},
		[]string{"group", "user", "datasource"})
)

func subscribeMon(sessions chan chan session, mon_messages chan<- amqp.Delivery, topic string) {

	for session := range sessions {
		sub := <-session

		go func() {
			ch, err := sub.Channel()
			if err != nil {
				logger.Fatalf("cannot create channel: %v", err)
				return
			}

			for range time.NewTicker(time.Duration(viper.GetInt("monitor_interval")) * time.Second).C {
				for k := range viper.Get("datasource").(map[string]interface{}) {
					if viper.GetBool(fmt.Sprintf("datasource.%s.write", k)) {
						for k2 := range viper.Get("datasource").(map[string]interface{}) {
							if k2 != k {
								routingKey := fmt.Sprintf("%s.%s", k2, k)

								inspectQueue, err := ch.QueueInspect("file." + routingKey)
								if err != nil {
									logger.Errorf("Cannot create queue %s: %v", "file."+routingKey, err)
									return
								}
								QueueMessagesGauge.WithLabelValues(routingKey, "file").Set(float64(inspectQueue.Messages))

								inspectQueue, err = ch.QueueInspect("dir." + routingKey)
								if err != nil {
									logger.Errorf("Cannot create queue %s: %v", "dir."+routingKey, err)
									return
								}
								QueueMessagesGauge.WithLabelValues(routingKey, "dir").Set(float64(inspectQueue.Messages))
							}
						}
					}
				}
				if viper.GetBool("scan") || viper.GetBool("scan_update") {
					for k := range viper.Get("datasource").(map[string]interface{}) {
						routingKey := fmt.Sprintf("%s", k)

						inspectQueue, err := ch.QueueInspect("file." + routingKey)
						if err != nil {
							logger.Errorf("Cannot create queue %s: %v", "file."+routingKey, err)
							return
						}
						QueueMessagesGauge.WithLabelValues(routingKey, "file").Set(float64(inspectQueue.Messages))

						inspectQueue, err = ch.QueueInspect("dir." + routingKey)
						if err != nil {
							logger.Errorf("Cannot create queue %s: %v", "dir."+routingKey, err)
							return
						}
						QueueMessagesGauge.WithLabelValues(routingKey, "dir").Set(float64(inspectQueue.Messages))
					}
				}
			}
		}()

		ch, err := sub.Channel()
		if err != nil {
			logger.Fatalf("cannot create channel: %v", err)
			continue
		}

		var wg sync.WaitGroup

		queueMon, err := ch.QueueDeclare("", false, true, true, true, nil)
		if err != nil {
			logger.Errorf("cannot consume from exclusive queue: %q, %v", queueMon, err)
			return
		}

		if err := ch.QueueBind(queueMon.Name, topic, "amq.topic", false, nil); err != nil {
			logger.Errorf("cannot consume without a binding to exchange: %q, %v", "amq.topic", err)
			return
		}

		deliveriesMon, err := ch.Consume(queueMon.Name, "", false, false, false, false, nil)
		if err != nil {
			logger.Errorf("cannot consume from: %q, %v", queueMon, err)
			return
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range deliveriesMon {
				mon_messages <- msg
				ch.Ack(msg.DeliveryTag, false)
			}
		}()
		wg.Wait()
	}
}

func processMonitorStream() chan<- amqp.Delivery {
	msgs := make(chan amqp.Delivery)
	go func() {
		for msg := range msgs {
			var curMonMessage monMessage

			buf := bytes.NewBuffer(msg.Body)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&curMonMessage)
			if err != nil {
				logger.Printf("Error parsing message: %s", err)
				continue
			}
			if curMonMessage.FilesCopied > 0 {
				FilesCopiedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FilesCopied)
			}
			if curMonMessage.FilesRemoved > 0 {
				FilesRemovedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FilesRemoved)
			}
			if curMonMessage.FilesSkipped > 0 {
				FilesSkippedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FilesSkipped)
			}
			if curMonMessage.FilesIndexed > 0 {
				FilesIndexedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FilesIndexed)
			}
			if curMonMessage.BytesTransferred > 0 {
				BytesCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.BytesTransferred)
			}
			if curMonMessage.FoldersCopied > 0 {
				FoldersCopiedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FoldersCopied)
			}

		}
	}()
	return msgs
}
