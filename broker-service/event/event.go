package event

import amqp "github.com/rabbitmq/amqp091-go"

func declareExchange(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		"logs_topic",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func declareRandomQueue(ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}
