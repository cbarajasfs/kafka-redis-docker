## Kafka Redis Docker
### Contents
- [Reason](#reason)
- [Kafka Installation](#kafka-installation)
- [Advantages](#advantages)
- [Disadvantages](#disadvantages)
- [Simple Way](#simple-instalation)
- [Confluent Way](#confluent-installation)


## Reason
Be able to connect Kafka to Redis using Kafka Connectors. There are two types of [connectors](https://docs.confluent.io/platform/current/connect/index.html#how-kafka-connect-works):

- **Source connector** – Ingests entire databases and streams table updates to Kafka topics. A source connector can also collect metrics from all your application servers and store these in Kafka topics, making the data available for stream processing with low latency.

- **Sink connector** – Delivers data from Kafka topics into secondary indexes such as Elasticsearch, Redis or batch systems such as Hadoop for offline analysis.

In this case we are going to implement a Sink Connector, At the moment there are two ways to achieve this, one directly installing the connector jar and the other way is relying on the technology of Confluent Hub.

## Advantages
we can stream data from Kafka topics to a target store (Sink Connectors) or stream updates in a data store to Kafka topics (Source Connectors), without making any line of code, which we would need to maintain
integrates optimally with the stream data platform
connectors can be managed through Confluent Control Center
we may use a REST API to create, update and delete Kafka connectors
we can easily integrate with Confluent Schema Registry, which provides centralized schema/APIs management and compatibility
a nice and clean way of deployment (for instance if we are using Kubernetes, we can define Helm charts for our dockerized connectors)
## Disadvantages 
the available connectors may have some limitation (sometimes we will need to deep dive into the connector’s documentation and check does it will meet with our requirements)
separation of commercial and open-source features is very poor
connectors documentation could be poor and may have some lacks
often no information about which connectors have been battle-tested in a real-world application.

## Versions

Main docker images used:
Name                   | Docker Image                     | Version 
-----------------------|----------------------------------|-----------------------
Kafka                  | zookeeper                        | 3.4.9
Zookeper               | confluentinc/cp-kafka            | 5.5.1
KafkaSchemeRegistry    | confluentinc/cp-schema-registry  | 5.5.1
KafkaRestProxy         | confluentinc/cp-kafka-rest       | 5.5.1
KafkaConnect           | confluentinc/cp-kafka-connect    | 5.5.1
KSLQDBServer           | confluentinc/cp-ksqldb-server    | 6.1.1


## Confluent Way

Start running docker compose with the next command

```bash
    docker-compose up -d --build --remove-orphans
```

What docker does, it that it will create and start the next images:

- Zookeper, Kafka Nodes Management
- Kafka Broker, Kafka server
- Redis, Redis image
- Confluent Control Center, GUI tool for managing and monitoring Apache Kafka
- Kafka Setup, in charge of adding de topics needed for Kafka Connect
- Kafka Connect, Runs Kafka Connect and installing the main two connectors for this case.

## Kafka Connectors 
Docker image Kafka Connect, will install through Confluent Hub(Similar to Github, Confluent has several libraries available in their Confluent Hub), the next connectors:

### DataGen configuration
This Connector will mock data certain time, for the exercise to proof that redis is receiving data.

Installation

Through REST API you can configure the connectors:
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @datagen-config.json http://localhost:8088/connectors
```
datagen-config.json located in the same folder contains:
```bash
{
    "name": "datagen",
    "config": {
      "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
      "kafka.topic": "to-redis",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "quickstart": "users",
      "max.interval": 10000,
      "iterations": 1000000
    }
}
```

### Kafka Connect Redis configuration
This is a Kafka Sink Connector, which means that every  message received in Kafka will be stored in Redis.


Installation

Through REST API you can configure the connectors:
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @redis-sink-config.json http://localhost:8088/connectors
```
redis-sink-config.json located in the same folder contains:
```bash
{
    "name": "redis-sink",
    "config": {
      "connector.class": "com.github.jcustenborder.kafka.connect.redis.RedisSinkConnector",
      "tasks.max": "1",
      "topics":"to-redis",
      "redis.hosts":"redis:6379"
    }
}
```


## Verification
Once both connectors are installed and configured, you can track the activity of your Kafka using Confluent Control Center in http://localhost:9021/


First we check if the connecto DataGen is sending messages and if Kafka is receiving.
![alt text](kafka-confluent/docs/Kafka%20001.PNG)

Second in Consumers we can see the load and status of our Redis Connector.
![alt text](kafka-confluent/docs/Kafka%20002.PNG)

Third within Redis we can check the data sended form Kafka.
![alt text](kafka-confluent/docs/Kafka%20003.PNG)


