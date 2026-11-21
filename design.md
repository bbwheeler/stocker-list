# stocker-list

A Go service that pulls stock symbols from the internet and publishes each one
to a RabbitMQ exchange as a `stockstorev1.Stock` protobuf message. It performs
exactly one full refresh cycle per process invocation and exits; scheduling is
external (a Kubernetes CronJob).

## RabbitMQ

Messages are published via AMQP to the durable exchange `stock.update` (configurable with
`RABBITMQ_EXCHANGE`, type `topic` via `RABBITMQ_EXCHANGE_TYPE`) using the routing key
`stock.update.v1` (configurable with `RABBITMQ_ROUTING_KEY`). The publisher declares only the
exchange; consumers own their queues and bindings.

The message should be defined using protobuf: `stockstorev1.Stock` contains the stock symbol, the exchange, and the timestamp. Optionally it can contain the company name, and any other appropriate fields.

Delivery is persistent (survives a broker restart when the consumer's queue is durable), and
publisher confirms are enabled: every publish waits for the broker's confirmation, and a failed
confirm reconnects and republishes once before failing the run.
