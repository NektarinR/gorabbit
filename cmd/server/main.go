package main

import (
	"log"
	amqp "github.com/streadway/amqp"
	"os"
)

func main(){
	conn, err := amqp.Dial("amqp://guest:guest@localhost:30001/")
	if err != nil{
		panic(err)
	}
	defer func() {
		if !conn.IsClosed(){
			conn.Close()
		}
	}()
	ch, err := conn.Channel()
	if err != nil{
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
	if err != nil{
		panic(err)
	}
	body := bodyFrom(os.Args)
	err = ch.Publish(
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil{
		panic(err)
	}
}