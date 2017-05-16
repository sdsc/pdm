package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"github.com/spf13/viper"
)

func readConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath("$HOME/.pdm")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

const exchange = "tasks"

type message struct {
	Body []byte
	RoutingKey string
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

func subscribe(sessions chan chan session, routingKey string, messages chan<- message) {

	for session := range sessions {
		sub := <-session

		queue, err := sub.QueueDeclare("", false, true, true, false, nil);
		if  err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
			return
		}

		fmt.Printf("%v",queue)

		if err := sub.QueueBind(queue.Name, routingKey, exchange, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
			return
		}

		deliveries, err := sub.Consume(queue.Name, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queue, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			var new_msg message
			new_msg.Body = msg.Body
			messages <- new_msg
			sub.Ack(msg.DeliveryTag, false)
		}
	}
}

func read(r io.Reader) <-chan message {
	lines := make(chan message)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			var msg message
			msg.Body = scan.Bytes()
			msg.RoutingKey = "panda.home"
			lines <- msg
		}
	}()
	return lines
}

func write(w io.Writer) chan<- message {
	msgs := make(chan message)
	go func() {
		for msg := range msgs {
			fmt.Fprintln(w, string(msg.Body))
		}
	}()
	return msgs
}

func main() {
	readConfig()

	ctx, done := context.WithCancel(context.Background())

	go func() {
		publish(redial(ctx, viper.GetString("rabbitmq.connect_string")), read(os.Stdin))
		done()
	}()

	go func() {
		for k := range viper.Get("datasource").(map[string]interface{}) {

			if(viper.GetBool(fmt.Sprintf("datasource.%s.write",k))) {
				log.Printf("%v is writeable",k)

				for k2 := range viper.Get("datasource").(map[string]interface{}) {
					if k2 != k {
						subscribe(redial(ctx, viper.GetString("rabbitmq.connect_string")), fmt.Sprintf("%s.%s",k2,k), write(os.Stdout))
					}
				}
			}
		}

		done()
	}()

	<-ctx.Done()
}