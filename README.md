# amqp-wrapper

This module is a wrapper for rabbitmq/amqp091-go package

## Installation

```bash
go get -u github.com/astreter/amqpwrapper/v2
```

## Publishing messages to a queue example

```go
import (
    "context"
    "github.com/astreter/amqpwrapper/v2"
    "sync"
)

func main() {
    ctx, cancelF := context.WithCancel(context.Background())
    
    wg := new(sync.WaitGroup)
    
    amqp, err := amqpwrapper.NewRabbitChannel(
        ctx,
        wg, // application can wait until all ongoing deliveries are processed
        &amqpwrapper.Config{
            URL: "amqp://user:password@localhost:5672",
            Debug: true, // true - amqpwrapper writes logs about each delivery
            ConfirmSends: true // true - the publisher waits for confirmation from RabbitMQ server that a message has been delivered 
        },
    )
    if err != nil {
        panic(err)
    }
    
    go func() {
        <-amqp.Cancel() // such channel informs that amqp was lost and failed to reconnect
        cancelF()
    }()

	request := map[string]interface{}{"send": "something"}
    if err = amqp.Publish(context.Background(), request, "exchange_name", "routing_key"); err != nil {
        panic(err)
    }

    <-ctx.Done()
    wg.Wait()
}
```

## Receiving messages from a queue example

```go
import (
    "context"
    "github.com/astreter/amqpwrapper/v2"
    "sync"
)

func main() {
    ctx, cancelF := context.WithCancel(context.Background())
    
    wg := new(sync.WaitGroup)
    
    amqp, err := amqpwrapper.NewRabbitChannel(
        ctx,
        wg, // application can wait until all ongoing deliveries are processed
        &amqpwrapper.Config{
            URL: "amqp://user:password@localhost:5672",
            Debug: true, // true - amqpwrapper writes logs about each delivery
            ConfirmSends: true // true - the publisher waits for confirmation from RabbitMQ server that a message has been delivered 
        },
    )
    if err != nil {
        panic(err)
    }
    
    go func() {
        <-amqp.Cancel() // such channel informs that amqp was lost and failed to reconnect
        cancelF()
    }()


    if err = amqp.DefineExchange(
		"exchange_name",
		false, // false - creates new exchange if it doesn't exist yet; true - relies on such exchange already exists
    ); err != nil {
        panic(err)
    }

    if err = amqp.SetUpConsumer(
        "exchange_name",
        "routing_key",
        func(ctx context.Context, delivery amqp.Delivery){
            //todo: process the delivery
        },
        amqpwrapper.WithOptionThreads(5), // number of threads which can process deliveries in parallel
    ); err != nil {
        panic(err)
    }

    <-ctx.Done()
    wg.Wait()
}
```
