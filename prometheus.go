package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type monMessage struct {
	DataSource       string
	Node             string
	FilesCopied      float64
	FilesSkipped     float64
	BytesTransferred float64
	FoldersCopied    float64
}

var (
	FilesCopiedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "files_copied_number",
		Help: "Number of files copied",
	},
		[]string{"node", "datasource"})
	FilesSkippedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "files_skipped_number",
		Help: "Number of files skipped",
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

func subscribeMon(sessions chan chan session, mon_messages chan<- amqp.Delivery, topic string) {

	for session := range sessions {
		sub := <-session

		go func() {
			ch, err := sub.Channel()
			if err != nil {
				log.Fatalf("cannot create channel: %v", err)
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
									log.Errorf("Cannot create queue %s: %v", "file."+routingKey, err)
									return
								}
								QueueMessagesGauge.WithLabelValues(routingKey, "file").Set(float64(inspectQueue.Messages))

								inspectQueue, err = ch.QueueInspect("dir." + routingKey)
								if err != nil {
									log.Errorf("Cannot create queue %s: %v", "dir."+routingKey, err)
									return
								}
								QueueMessagesGauge.WithLabelValues(routingKey, "dir").Set(float64(inspectQueue.Messages))
							}
						}
					}
				}
			}
		}()

		ch, err := sub.Channel()
		if err != nil {
			log.Fatalf("cannot create channel: %v", err)
			continue
		}

		var wg sync.WaitGroup

		queueMon, err := ch.QueueDeclare("", false, true, true, true, nil)
		if err != nil {
			log.Errorf("cannot consume from exclusive queue: %q, %v", queueMon, err)
			return
		}

		if err := ch.QueueBind(queueMon.Name, topic, "amq.topic", false, nil); err != nil {
			log.Errorf("cannot consume without a binding to exchange: %q, %v", "amq.topic", err)
			return
		}

		deliveriesMon, err := ch.Consume(queueMon.Name, "", false, false, false, false, nil)
		if err != nil {
			log.Errorf("cannot consume from: %q, %v", queueMon, err)
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
				log.Printf("Error parsing message: %s", err)
				continue
			}
			if curMonMessage.FilesCopied > 0 {
				FilesCopiedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FilesCopied)
			}
			if curMonMessage.FilesSkipped > 0 {
				FilesSkippedCounter.WithLabelValues(curMonMessage.Node, curMonMessage.DataSource).Add(curMonMessage.FilesSkipped)
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
