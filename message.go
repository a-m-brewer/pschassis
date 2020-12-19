package pschassis

import (
	"strconv"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessagePayload is an interface for creating the contents of a message
type MessagePayload interface {
	ToByte() ([]byte, error)
	EntityID() int
}

// MessageBuilder can be used to easily create new messages
type MessageBuilder interface {
	CreateMessage(m MessagePayload) (*message.Message, error)
}

// ServiceMessageBuilder is a
type ServiceMessageBuilder struct {
	service *Service
}

// NewServiceMessageBuilder create a new MessageBuilder
func NewServiceMessageBuilder(service *Service) *ServiceMessageBuilder {
	return &ServiceMessageBuilder{service: service}
}

// CreateMessage generates a new message
func (s *ServiceMessageBuilder) CreateMessage(m MessagePayload) (*message.Message, error) {
	bytes, err := m.ToByte()
	if err != nil {
		return nil, err
	}

	meta := map[string]string{
		"entityId": strconv.Itoa(m.EntityID()),
		"source":   s.service.name,
	}

	return &message.Message{
		UUID:     watermill.NewUUID(),
		Metadata: meta,
		Payload:  bytes,
	}, err
}
