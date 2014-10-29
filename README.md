Divolte Kafka Consumer
======================

Helper library for writing Kafka consumers that process events created by [Divolte Collector](https://github.com/divolte/divolte-collector). Divolte Collector captures click stream data and translates events into Avro records which are published on Kafka topics. The contents of these messages are the raw bytes produced by serializing the Avro records. This library allows to create consumers for these events in Java in a typesafe manner with minimal boilerplate.

To use the consumer, you need to generate Java code from your Avro schema. See [divolte-examples/avro-schema](https://github.com/divolte/divolte-examples/tree/master/avro-schema) for an example of a project that uses Maven to build a jar with generated code from a schema.

## Usage

Example consumer code:
```java
public class ConsumerExample {
    // Event handler that prints records to stdout
    static class MyEventHandler implements SimpleEventHandler<MyEventRecord> {
        @Override
        public void handle(MyEventRecord event) throws Exception {
            System.out.println(event);
        }
    }

    // Supplier of event handler instances
    static class MyEventHandlerSupplier implements Supplier<SimpleEventHandler<MyEventRecord>> {
        @Override
        public SimpleEventHandler<MyEventRecord> get() {
            return new MyEventHandler();
        }
    }

    public static void main(String[] args) {
        // Create the consumer
        // MyEventRecord is generated by the Avro Java code generator
        DivolteKafkaConsumer<MyEventRecord> consumer = DivolteKafkaConsumer.createConsumerWithSimpleHandler(
                "divolte",                             // Kafka topic
                "zk1:2181,zk2:2181,zk3:2181",          // Zookeeper quorum hosts + ports
                "my-consumer-group",                   // Kafka consumer group ID
                2,                                     // Number of threads for this consumer instance
                new MyEventHandlerSupplier(),          // Supplier of event handler instances
                MyEventRecord.getClassSchema());       // Avro schema

        // Add a shutdown hook that stops the consumer
        // This handles CTRL+C or kill
        Runtime.getRuntime().addShutdownHook(new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        consumer.shutdownConsumer();
                    }
                }));

        // Start the consumer
        consumer.startConsumer();
    }
}
```

If you are using Java 8, the above example can be condensed to this:
```java
public class ConsumerExample {
    public static void main(String[] args) {
        // Create the consumer
        DivolteKafkaConsumer<MyEventRecord> consumer = DivolteKafkaConsumer.createConsumerWithSimpleHandler(
                "divolte",                           // Kafka topic
                "zk1:2181,zk2:2181,zk3:2181",        // Zookeeper quorum hosts + ports
                "my-consumer-group",                 // Kafka consumer group ID
                2,                                   // Number of threads for this consumer instance
                () -> (e) -> System.out.println(e),  // Supplier of event handler instances
                MyEventRecord.getClassSchema());     // Avro schema

        // Add a shutdown hook that stops the consumer
        // This handles CTRL+C or kill
        Runtime
        .getRuntime()
        .addShutdownHook(
                new Thread(consumer::shutdownConsumer)
                );

        // Start the consumer
        consumer.startConsumer();
    }
}
```

For a more complete usage example, have a look at [divolte-examples/tcp-kafka-consumer](https://github.com/divolte/divolte-examples/tree/master/tcp-kafka-consumer).

## Build form source
We use [Gradle](http://www.gradle.org/) as a build tool. You need Java 7 or higher to build.

To build from source on your machine:

```sh
# cd into your preferred working dir
git clone https://github.com/divolte/divolte-kafka-consumer.git
cd divolte-kafka-consumer

# build the source
./gradlew build

# generate Eclipse project files
./gradlew eclipse

# install into your local Maven repository
./gradlew install
```
