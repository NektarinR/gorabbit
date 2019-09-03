package main

import (
	"bufio"
	"fmt"
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
	q := initQueue(ch)
	msgs := initDelivery(ch, q)
	go getAnswer(msgs)
	stdioMsg := make(chan string, 10)
	log.Println("Клиент запущен")
	go getMessageFromIO(stdioMsg)
	initExchange(ch)
	go sendMessageToExchange(stdioMsg, ch, q)
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT)
	<-exit
	close(stdioMsg)
}

func sendMessageToExchange(input <-chan string, ch *amqp.Channel, q *amqp.Queue) {
	for txt := range input {
		err := ch.Publish(
			"logs", // exchange
			"",     // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ReplyTo:     q.Name,
				ContentType: "text/plain",
				Body:        []byte(txt),
			})
		if err != nil {
			panic(err)
		}
	}
}

func initExchange(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
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
}

func getMessageFromIO(output chan string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Введите: ")
		symb, _ := reader.ReadString('\n')
		output <- symb[:len(symb)-1]
		go fmt.Printf("Клиент отправил: %s\n", symb[:len(symb)-1])
	}
}

func getAnswer(answerFromRPC <-chan amqp.Delivery) {
	for ms := range answerFromRPC {
		log.Printf("Клиент получил: %s", string(ms.Body))
	}
}

func initDelivery(ch *amqp.Channel, q *amqp.Queue) <-chan amqp.Delivery {
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
	return msgs
}

func initQueue(ch *amqp.Channel) *amqp.Queue {
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}
	return &q
}
