package jxrabbitmq

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/publisher"
	amqp "github.com/rabbitmq/amqp091-go"
)

var dialer *amqpextra.Dialer
var p *publisher.Publisher

type Config struct {
	Url      string
	Username string
	Password string
}

func InitFromEV() error {
	var err error
	urls := make([]string, 0)
	for _, v := range strings.Split(os.Getenv("RABBITMQ_POOL"), ",") {
		urls = append(urls, "amqp://"+os.Getenv("RABBITMQ_USER")+":"+
			os.Getenv("RABBITMQ_PASS")+"@"+
			v+"/")
	}

	dialer, err = amqpextra.NewDialer(amqpextra.WithURL(urls...))
	if err != nil {
		return err
	}

	p, err = dialer.Publisher()
	if err != nil {
		return err
	}
	
	return nil
}

func InitWithConfig(cf *Config) error {
	urls := make([]string, 0)
	for _, v := range strings.Split(cf.Url, ",") {
		urls = append(urls, "amqp://"+cf.Username+":"+cf.Password+"@"+v+"/")
	}
	var err error
	dialer, err = amqpextra.NewDialer(
		amqpextra.WithURL(urls...),
		amqpextra.WithRetryPeriod(3*time.Second),
	)
	if err != nil {
		return err
	}

	p, err = dialer.Publisher()
	if err != nil {
		return err
	}

	return nil
}

func GetPublisher() *publisher.Publisher {
	return p
}

func Publish(ctx context.Context, queue string, headers map[string]interface{}, body interface{}) error {
	bytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	message := publisher.Message{
		Context: ctx,
		Key:     queue,
		Publishing: amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes,
		},
	}
	if len(headers) > 0 {
		message.Publishing.Headers = headers
	}
	return p.Publish(message)
}

func PublishNoHeader(ctx context.Context, queue string, body interface{}) error {
	bytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	message := publisher.Message{
		Context: ctx,
		Key:     queue,
		Publishing: amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes,
		},
	}

	return p.Publish(message)
}
