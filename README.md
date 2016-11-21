# Scaling out with Kafka Consumer Groups

A simple example to demonstrate how Kafka consumers are designed for distributed, scale-out architectures

## Scenario

- single node cluster (keeping things simple)
- 4 partitions
- start with 1 consumer and bump up to 4 consumers (increment by 1)
- start Kafka
- download and setup code ```mvn clean install```

## Actions & observations

- start the producer (we'll keep one producer for simplicity) - *<DOWNLOAD_DIR>/target/java -jar kafka-consumer-group-test-jar-with-dependencies.jar producer*
- start all consumers one by one and keep track of the logs in order to figure out partition load distribution
  - start consumer instance #1 - *<DOWNLOAD_DIR>/target/java -jar kafka-consumer-group-test-jar-with-dependencies.jar consumer*
  
Check out the [blog post](https://simplydistributed.wordpress.com/2016/11/21/scaling-out-with-kafka-consumer-groups) for more details
