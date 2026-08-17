# stocker-list

A Go service that pulls stock information from the internet and sends each one out on a kafka topic. On a timer, it adds new
listings.

## Kafka

The kafka message should be defined using protobuf. The message should contain: The stock symbol, the exchange, and the timestamp. Optionally it can contain the company name, and any other appropriate fields.