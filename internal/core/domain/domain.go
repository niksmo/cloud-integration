package domain

import "github.com/google/uuid"

type Payment struct {
	ID     string
	Name   string
	Amount float64
}

func NewPayment(name string, amount float64) Payment {
	return Payment{
		ID:     uuid.NewString(),
		Name:   name,
		Amount: amount,
	}
}
