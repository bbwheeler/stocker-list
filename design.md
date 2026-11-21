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

Messages use the `stockstorev1.Stock` protobuf from `stocker-store`. The publisher sets `Symbol` and `Exchange` (leaving `Scores` empty); the publish time travels in the AMQP `Timestamp` property, and `symbol`/`exchange` are also copied into message headers.

Delivery is persistent (survives a broker restart when the consumer's queue is durable), and
publisher confirms are enabled: every publish waits for the broker's confirmation, and a failed
confirm reconnects and republishes once before failing the run.
