package main

import (
	"encoding/gob"
	"bytes"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"log"
	"github.com/streadway/amqp"
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
)

func subscribeMon(sessions chan chan session, mon_messages chan<- amqp.Delivery, topic string) {

	for session := range sessions {
		sub := <-session

		ch, err := sub.Channel()
		if err != nil {
			log.Fatalf("cannot create channel: %v", err)
			continue
		}

		var wg sync.WaitGroup

		queueMon, err := ch.QueueDeclare("", false, true, true, true, nil)
		if err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queueMon, err)
			return
		}

		if err := ch.QueueBind(queueMon.Name, topic, "amq.topic", false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", "amq.topic", err)
			return
		}

		deliveriesMon, err := ch.Consume(queueMon.Name, "", false, false, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queueMon, err)
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
			debug("Got new message %v", curMonMessage)
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
