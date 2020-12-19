package pschassis

import (
	"context"
	"log"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// RmqCqrs sets up all the boilerplate needed to implement cqrs with watermill
// For ps microservices
type RmqCqrs struct {
	address         string
	commandHandlers func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.CommandHandler
	eventHandlers   func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler
	facade          *cqrs.Facade
	router          *message.Router
}

// NewRmqCqrs creates a new cqrs instance
func NewRmqCqrs(
	address string,
	commandHandlers []cqrs.CommandHandler,
	eventHandlers []cqrs.EventHandler) *RmqCqrs {

	i := RmqCqrs{
		address: address,
		commandHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.CommandHandler {
			return commandHandlers
		},
		eventHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler {
			return eventHandlers
		},
	}
	i.new()

	return &i
}

// NewRmqCqrsPublisher creates a cqrs bus with no subscribers just for publishing
func NewRmqCqrsPublisher(address string) *RmqCqrs {
	i := RmqCqrs{
		address: address,
	}
	i.new()
	return &i
}

// NewRmqCqrsPubSub creates a RmqCqrs where you have access to the event and command publishers
func NewRmqCqrsPubSub(
	address string,
	commandHandlers func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.CommandHandler,
	eventHandlers func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler) *RmqCqrs {

	i := RmqCqrs{
		address:         address,
		commandHandlers: commandHandlers,
		eventHandlers:   eventHandlers,
	}
	i.new()

	return &i
}

// Facade Gets the watermill facade
func (r *RmqCqrs) Facade() *cqrs.Facade {
	return r.facade
}

// Router gets the watermill router
func (r *RmqCqrs) Router() *message.Router {
	return r.router
}

// Start the cqrs bus
func (r *RmqCqrs) Start() error {
	return r.router.Run(context.Background())
}

func (r *RmqCqrs) new() {
	wmLogger := watermill.NewStdLogger(false, false)
	cqrsMarhaler := cqrs.JSONMarshaler{}

	commandCfg := amqp.NewDurableQueueConfig(r.address)
	eventCfg := amqp.NewDurablePubSubConfig(r.address, nil)

	commandsPublisher, err := amqp.NewPublisher(commandCfg, wmLogger)
	if err != nil {
		wmLogger.Error("creating command publisher", err, watermill.LogFields{})
		return
	}

	eventsPublisher, err := amqp.NewPublisher(eventCfg, wmLogger)
	if err != nil {
		wmLogger.Error("creating event publisher", err, watermill.LogFields{})
		return
	}

	commandsSubscriber, err := amqp.NewSubscriber(commandCfg, wmLogger)
	if err != nil {
		log.Fatal(err)
	}

	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	router, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		log.Fatal(err)
	}

	router.AddMiddleware(middleware.Recoverer)

	// cqrs.Facade is facade for Command and Event buses and processors.
	// You can use facade, or create buses and processors manually (you can inspire with cqrs.NewFacade)
	cqrsFacade, err := cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateCommandsTopic: func(commandName string) string {
			// we are using queue RabbitMQ config, so we need to have topic per command type
			return commandName
		},
		CommandHandlers:   r.commandHandlers,
		CommandsPublisher: commandsPublisher,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			// we can reuse subscriber, because all commands have separated topics
			return commandsSubscriber, nil
		},
		GenerateEventsTopic: func(eventName string) string {
			// because we are using PubSub RabbitMQ config, we can use one topic for all events
			return "events"
		},
		EventHandlers:   r.eventHandlers,
		EventsPublisher: eventsPublisher,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			config := amqp.NewDurablePubSubConfig(
				r.address,
				amqp.GenerateQueueNameTopicNameWithSuffix(handlerName),
			)

			return amqp.NewSubscriber(config, wmLogger)
		},
		Router:                router,
		CommandEventMarshaler: cqrsMarhaler,
		Logger:                wmLogger,
	})
	if err != nil {
		log.Fatal(err)
	}

	r.facade = cqrsFacade
	r.router = router
}
