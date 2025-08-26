package schema

import (
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"
)

const PaymentSchemaTextV1 = `{
	"type": "record",
	"namespace": "transactions",
	"name": "payment",
	"fields" : [
		{"name": "id", "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "amount", "type": "double"}
	]
}`

type PaymentV1 struct {
	ID     string  `avro:"id"`
	Name   string  `avro:"name"`
	Amount float64 `avro:"amount"`
}

var PaymentSchemaV1 = sr.Schema{
	Type:   sr.TypeAvro,
	Schema: PaymentSchemaTextV1,
}

func PaymentV1Avro() avro.Schema {
	s, err := avro.Parse(PaymentSchemaTextV1)
	if err != nil {
		err = fmt.Errorf(
			"failed to parse PaymentSchemaTextV1, contact with package dev team: %w",
			err,
		)
		panic(err)
	}
	return s
}

func PaymentV1AvroEncodeFn() func(v any) ([]byte, error) {
	return func(v any) ([]byte, error) {
		s := PaymentV1Avro()
		return avro.Marshal(s, v)
	}
}

func PaymentV1AvroDecodeFn() func([]byte, any) error {
	return func(data []byte, v any) error {
		s := PaymentV1Avro()
		return avro.Unmarshal(s, data, v)
	}
}
