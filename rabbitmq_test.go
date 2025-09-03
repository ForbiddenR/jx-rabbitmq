package jxrabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestParseURI(t *testing.T) {
	test := `amqp://admin:123!abc456,.@172.16.0.201:5672/`
	url, err := amqp.ParseURI(test)
	assert.Nil(t, err)
	t.Log(test)
	t.Log(url)
}
