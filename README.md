# Getting Started Examples
## Solace Messaging API for Java JCSMP - Special Edition: DIY Partitioned Queues

## Prerequisites

This project is designed to highlight a client-side implementation of Partitioned Queue functionality with minimal modification to the dependent code.

Assume that the application code expects a queue `q_jcsmp_sub` with subscription `solace/samples/jcsmp/pers/pub` with 4 partitions. In the broker manager, create the following **exclusive** queues and subscriptions:

|  Queue Name    |  Topic Subscription               |
|----------------|-----------------------------------|
|`q_jcsmp_sub`   |               (None)              |
|`q_jcsmp_sub/1` | `solace/samples/jcsmp/pers/pub/1` |
|`q_jcsmp_sub/2` | `solace/samples/jcsmp/pers/pub/2` |
|`q_jcsmp_sub/3` | `solace/samples/jcsmp/pers/pub/3` |
|`q_jcsmp_sub/4` | `solace/samples/jcsmp/pers/pub/4` |

The numbered queues and topics are not used by the main application code directly, but rather are an implementation detail of the helper polyfill classes found in `/src/polyfill`.

In `GuaranteedPublisher.java` and `GuaranteedSubscriber.java` there is a constant variable, `PARTITION_COUNT`, which should match the number of partitions created on the broker.

## Build the Samples

Just clone and build. For example:

  1. clone this GitHub repository
```
git clone https://github.com/SolaceSamples/solace-samples-java-jcsmp
cd solace-samples-java-jcsmp
```
  2. Build samples
  
  ```
  ./gradlew assemble
  ```

## Running the Samples

To try individual samples, build the project from source and then run samples like the following:

```
cd build/staged
bin/GuaranteedPublisher <host:port> <message-vpn> <client-username> [password]
bin/GuaranteedSubscriber <host:port> <message-vpn> <client-username> [password]
```

