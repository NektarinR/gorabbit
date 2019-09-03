package main

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:30001/")
	if err != nil {
		panic(err)
	}
	defer func() {
		if !conn.IsClosed() {
			conn.Close()
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	msg := make(chan string, 10)
	log.Println("Клиент запущен")
	go func(output chan<- string) {
		timer := time.NewTicker(5 * time.Millisecond)
		for v := range timer.C {
			output <- v.String()
		}
	}(msg)
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
	go func(input <-chan string) {
		for txt := range input {
			err = ch.Publish(
				"logs", // exchange
				"",     // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(txt),
				})
			if err != nil {
				panic(err)
			}
			go log.Printf("Отправил: %s\n", txt)
		}
	}(msg)
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT)
	<-exit
}
