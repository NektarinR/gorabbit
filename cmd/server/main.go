package main

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:30001/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		panic(err)
	}
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}
	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"logs", // exchange
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		panic(err)
	}
	go func(input <-chan amqp.Delivery) {
		log.Printf("Сервер запущен и слушает, очередь: %s\n", q.Name)
		for msg := range input {
			_ = msg
		}
	}(msgs)
	closeApp := make(chan os.Signal, 1)
	signal.Notify(closeApp, syscall.SIGINT)
	<-closeApp
}
