package rmq

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

var Dialer *amqpextra.Dialer
var P *publisher.Publisher

type Option struct {
	Url      string
	Username string
	Password string
}

func Init() error {
	urls := make([]string, 0)
	for _, v := range strings.Split(os.Getenv("RABBITMQ_POOL"), ",") {
		urls = append(urls, "amqp://"+os.Getenv("RABBITMQ_USER")+":"+
			os.Getenv("RABBITMQ_PASS")+"@"+
			v+"/")
	}

	var err error
	Dialer, err = amqpextra.NewDialer(
		amqpextra.WithURL(urls...),
		amqpextra.WithRetryPeriod(3*time.Second),
	)
	if err != nil {
		return err
	}
	P, err = Dialer.Publisher()
	if err != nil {
		return err
	}
	return nil
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
	return P.Publish(message)
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

	return P.Publish(message)
}
