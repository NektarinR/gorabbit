package main

import (
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

var (
	num *int
)

func main() {
	num = flag.Int("num", 0, "номер воркера")
	flag.Parse()
	conn := createConn()
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	initExchange(ch)
	q := initQueue(ch, strconv.Itoa(*num))
	fmt.Printf("Сервер запущен и слушает, очередь: %s\n", q.Name)
	msgs := getChanMessage(ch, q)
	go getMessage(msgs, ch)
	closeApp := make(chan os.Signal, 1)
	signal.Notify(closeApp, syscall.SIGINT)
	<-closeApp
}

func getChanMessage(ch *amqp.Channel, q *amqp.Queue) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	return msgs
}

func initQueue(ch *amqp.Channel, nameQueue string) *amqp.Queue {
	q, err := ch.QueueDeclare(
		nameQueue,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	err = ch.QueueBind(
		q.Name,
		"",
		"logs",
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	return &q
}

func initExchange(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"logs",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
}

func createConn() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:30001/")
	if err != nil {
		panic(err)
	}
	return conn
}

func getMessage(input <-chan amqp.Delivery, ch *amqp.Channel) {
	for msg := range input {
		fmt.Printf("Клиент прислал: %s\n", string(msg.Body))
		if string(msg.Body) == strconv.Itoa(*num) {
			answer := fmt.Sprintf("Сервер %d отправил %s", *num, string(msg.Body))
			err := ch.Publish(
				"",          // exchange
				msg.ReplyTo, // routing key
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(answer),
				})
			if err != nil {
				log.Println(err)
			}
		}
	}
}
