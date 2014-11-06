/*
 * Copyright 2014 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.divolte.kafka.consumer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Helper class for consuming messages from Kafka queues that are serialized
 * using Avro with a known schema; requires Avro's generated Java code for the
 * schema.
 * <p>
 * This class implements a Kafka consumer using the high level consumer API as
 * described <a href=
 * "https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example"
 * >here</a>. Kafka has the notion of consumer groups. Messages will be balanced
 * across all consumer thread in the same consumer group. This class creates one
 * or more consumer threads, which will each receive a portion of the total
 * message stream. If multiple consumer instances are started with the same
 * consumer group ID, messages will be distributed over these instances; these
 * instances do not have to reside within the same JVM or on the same physical
 * machine. When a consumer instance dies or shuts down, Kafka will rebalance
 * the distribution of messages over the remaining instances.
 * <p>
 * Clients of this class provide the consumer with an {@link EventHandler}
 * instance, which handles individual messages. This class takes care of
 * deserializing the raw messages into Avro specific records, implemented in the
 * generated code.
 *
 * @param <T>
 *            The class that was generated from the Avro schema used for
 *            serialization.
 */
public final class DivolteKafkaConsumer<T extends SpecificRecord> {
    private final static Logger logger = LoggerFactory.getLogger(DivolteKafkaConsumer.class);

    public final static long DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 400;
    public final static long DEFAULT_ZOOKEEPER_SYNC_TIMEOUT = 200;
    public final static long DEFAULT_AUTO_COMMIT_INTERVAL = 1000;

    private final int numThreads;
    private final ConsumerConnector consumer;
    private final ExecutorService executorService;

    private final String topic;
    private final Supplier<EventHandler<T>> handlerSupplier;
    private final Schema schema;

    /**
     * Create a consumer with one or more consumer threads that consumes
     * messages from a Kafka topic and belongs to a specific consumer group. A
     * consumer thread uses a supplied {@link EventHandler} to handle the
     * messages after deserialization. The {@link EventHandler} is supplied to
     * the consumer through a {@link Supplier} instance passed to this
     * constructor. This way, the consumer can create one {@link EventHandler}
     * instance per consumer thread, so clients do not have to create
     * {@link EventHandler}'s that are thread safe. The provided
     * {@link Supplier#get()} will be called at least once per created thread,
     * but can be called more often in case of uncaught exceptions in
     * {@link EventHandler}'s. When an {@link EventHandler} throws a uncaught
     * exception, it will be discarded (after {@link EventHandler#shutdown()}
     * has been called) and a new instance will be requested.
     *
     * @param topic
     *            Kafka topic to consume.
     * @param zookeeper
     *            Zookeeper connect string (e.g.
     *            "host1:2181,host2:2181,host3:2181")
     * @param groupId
     *            Kafka consumer group ID.
     * @param numThreads
     *            Number of consumer threads to create for this consumer.
     * @param handlerSupplier
     *            A {@link Supplier} of {@link EventHandler} instances.
     * @param schema
     *            The Avro {@link Schema} to be used for deserializing raw
     *            messages.
     * @param zookeeperSessionTimeoutMs
     *            The zookeeper session timeout used by the client; this is used
     *            for the 'zookeeper.session.timeout.ms' property in the Kafka
     *            client configuration.
     * @param zookeeperSyncTimeMs
     *            The zookeeper sync time used by the client; this is used for
     *            the 'zookeeper.sync.time.ms' property in the Kafka client
     *            configuration.
     * @param autoCommitIntervalMs
     *            The zookeeper auto commit interval used by the client; this is
     *            used for the 'auto.commit.interval.ms' property in the Kafka
     *            client configuration.
     * @param <T>
     *            The class that was generated from the Avro schema used for
     *            serialization.
     * @return A new {@link DivolteKafkaConsumer} instance.
     */
    public static <T extends SpecificRecord> DivolteKafkaConsumer<T> createConsumer(
            final String topic,
            final String zookeeper,
            final String groupId,
            final int numThreads,
            final Supplier<EventHandler<T>> handlerSupplier,
            final Schema schema,
            final long zookeeperSessionTimeoutMs,
            final long zookeeperSyncTimeMs,
            final long autoCommitIntervalMs) {
        return new DivolteKafkaConsumer<>(topic, zookeeper, groupId, numThreads, handlerSupplier, schema, zookeeperSessionTimeoutMs, zookeeperSyncTimeMs, autoCommitIntervalMs);
    }

    /**
     * Construct a new {@link DivolteKafkaConsumer} by using the following
     * defaults:
     * {@link DivolteKafkaConsumer#DEFAULT_ZOOKEEPER_SESSION_TIMEOUT},
     * {@link DivolteKafkaConsumer#DEFAULT_ZOOKEEPER_SYNC_TIMEOUT},
     * {@link DivolteKafkaConsumer#DEFAULT_AUTO_COMMIT_INTERVAL}
     *
     * @see #createConsumer(String, String, String, int, Supplier, Schema, long, long, long)
     *
     * @param topic
     *            Kafka topic to consume.
     * @param zookeeper
     *            Zookeeper connect string (e.g.
     *            "host1:2181,host2:2181,host3:2181")
     * @param groupId
     *            Kafka consumer group ID.
     * @param numThreads
     *            Number of consumer threads to create for this consumer.
     * @param handlerSupplier
     *            A {@link Supplier} of {@link EventHandler} instances.
     * @param schema
     *            The Avro {@link Schema} to be used for deserializing raw
     *            messages.
     * @param <T>
     *            The class that was generated from the Avro schema used for
     *            serialization.
     * @return A new {@link DivolteKafkaConsumer} instance.
     */
    public static <T extends SpecificRecord> DivolteKafkaConsumer<T> createConsumer(
            final String topic,
            final String zookeeper,
            final String groupId,
            final int numThreads,
            final Supplier<EventHandler<T>> handlerSupplier,
            final Schema schema) {
        return new DivolteKafkaConsumer<T>(topic, zookeeper, groupId, numThreads, handlerSupplier, schema, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_SYNC_TIMEOUT, DEFAULT_AUTO_COMMIT_INTERVAL);
    }

    /**
     * Creates a {@link DivolteKafkaConsumer} using an event handler that
     * implements {@link SimpleEventHandler}. This event handler interface has
     * only one single method for handling events and lacks lifecycle management
     * methods. Specifically, this can be useful for creating event handlers
     * that require no setup or tear down or if you want to implement event
     * handlers using Java 8's lambda expressions.
     *
     * @see #createConsumer(String, String, String, int, Supplier, Schema)
     *
     * @param topic
     *            Kafka topic to consume.
     * @param zookeeper
     *            Zookeeper connect string (e.g.
     *            "host1:2181,host2:2181,host3:2181")
     * @param groupId
     *            Kafka consumer group ID.
     * @param numThreads
     *            Number of consumer threads to create for this consumer.
     * @param handlerSupplier
     *            A {@link Supplier} of {@link EventHandler} instances.
     * @param schema
     *            The Avro {@link Schema} to be used for deserializing raw
     *            messages.
     * @param <T>
     *            The class that was generated from the Avro schema used for
     *            serialization.
     * @return A new {@link DivolteKafkaConsumer} instance.
     */
    public static <T extends SpecificRecord>DivolteKafkaConsumer<T> createConsumerWithSimpleHandler(
            final String topic,
            final String zookeeper,
            final String groupId,
            final int numThreads,
            final Supplier<SimpleEventHandler<T>> handlerSupplier,
            final Schema schema) {
        return createConsumer(
                topic,
                zookeeper,
                groupId,
                numThreads,
                new Supplier<EventHandler<T>>() {
                    @Override
                    public EventHandler<T> get() {
                        return new SimpleEventHandlerWrapper<T>(handlerSupplier.get());
                    }
                },
                schema);
    }

    /**
     * Creates a {@link DivolteKafkaConsumer} using an event handler that
     * implements {@link SimpleEventHandler}. This event handler interface has
     * only one single method for handling events and lacks lifecycle management
     * methods. Specifically, this can be useful for creating event handlers
     * that require no setup or tear down or if you want to implement event
     * handlers using Java 8's lambda expressions.
     *
     * @see #createConsumer(String, String, String, int, Supplier, Schema)
     *
     * @param topic
     *            Kafka topic to consume.
     * @param zookeeper
     *            Zookeeper connect string (e.g.
     *            "host1:2181,host2:2181,host3:2181")
     * @param groupId
     *            Kafka consumer group ID.
     * @param numThreads
     *            Number of consumer threads to create for this consumer.
     * @param handlerSupplier
     *            A {@link Supplier} of {@link EventHandler} instances.
     * @param schema
     *            The Avro {@link Schema} to be used for deserializing raw
     *            messages.
     * @param <T>
     *            The class that was generated from the Avro schema used for
     *            serialization.
     * @param zookeeperSessionTimeoutMs
     *            The zookeeper session timeout used by the client; this is used
     *            for the 'zookeeper.session.timeout.ms' property in the Kafka
     *            client configuration.
     * @param zookeeperSyncTimeMs
     *            The zookeeper sync time used by the client; this is used for
     *            the 'zookeeper.sync.time.ms' property in the Kafka client
     *            configuration.
     * @param autoCommitIntervalMs
     *            The zookeeper auto commit interval used by the client; this is
     *            used for the 'auto.commit.interval.ms' property in the Kafka
     *            client configuration.
     * @return A new {@link DivolteKafkaConsumer} instance.
     */
    public static <T extends SpecificRecord>DivolteKafkaConsumer<T> createConsumerWithSimpleHandler(
            final String topic,
            final String zookeeper,
            final String groupId,
            final int numThreads,
            final Supplier<SimpleEventHandler<T>> handlerSupplier,
            final Schema schema,
            final long zookeeperSessionTimeoutMs,
            final long zookeeperSyncTimeMs,
            final long autoCommitIntervalMs) {
        return createConsumer(
                topic,
                zookeeper,
                groupId,
                numThreads,
                new Supplier<EventHandler<T>>() {
                    @Override
                    public EventHandler<T> get() {
                        return new SimpleEventHandlerWrapper<T>(handlerSupplier.get());
                    }
                },
                schema,
                zookeeperSessionTimeoutMs,
                zookeeperSyncTimeMs,
                autoCommitIntervalMs);
    }

    /**
     * Starts the consumer threads. Each consumer thread will request a
     * {@link EventHandler} from the provided {@link Supplier}.
     */
    public void startConsumer() {
        ImmutableMap<String, Integer> threadsPerTopicMap = ImmutableMap.of(Objects.requireNonNull(topic), numThreads);
        for(final KafkaStream<byte[], byte[]> stream : consumer.createMessageStreams(threadsPerTopicMap).get(topic)) {
            final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
            final SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
            scheduleReader(stream, decoder, reader);
        }
    }

    /**
     * Shutdown the consumer and subsequently, all consumer threads.
     */
    public void shutdownConsumer() {
        consumer.shutdown();
        executorService.shutdown();
    }

    /*
     * This class uses factory methods instead of constructors to make a better
     * distinction between the SimpleEventHandler<T> and the EventHandler<T>.
     */
    @SuppressWarnings("PMD.AvoidThreadGroup")
    private DivolteKafkaConsumer(
            final String topic,
            final String zookeeper,
            final String groupId,
            final int numThreads,
            final Supplier<EventHandler<T>> handlerSupplier,
            final Schema schema,
            final long zookeeperSessionTimeoutMs,
            final long zookeeperSyncTimeMs,
            final long autoCommitIntervalMs
            ) {
        this.topic = Objects.requireNonNull(topic);
        this.schema = Objects.requireNonNull(schema);
        this.handlerSupplier = Objects.requireNonNull(handlerSupplier);

        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config(
                Objects.requireNonNull(zookeeper),
                Objects.requireNonNull(groupId),
                zookeeperSessionTimeoutMs,
                zookeeperSyncTimeMs,
                autoCommitIntervalMs));

        this.numThreads = numThreads;
        final ThreadGroup threadGroup = new ThreadGroup("Consumer threads [" + groupId + "]");
        final ThreadFactory factory = createThreadFactory(threadGroup, "Consumer [" + groupId + "] - %d");
        this.executorService = Executors.newFixedThreadPool(numThreads, factory);
    }

    private static final class SimpleEventHandlerWrapper<T> implements EventHandler<T> {
        private final SimpleEventHandler<T> wrapped;
        SimpleEventHandlerWrapper(final SimpleEventHandler<T> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void handle(T event) throws Exception {
            wrapped.handle(event);
        }

        @Override
        public void setup() throws Exception { }

        @Override
        public void shutdown() throws Exception { }
    }

    private void scheduleReader(final KafkaStream<byte[], byte[]> stream, final BinaryDecoder decoder, final SpecificDatumReader<T> reader) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                final EventHandler<T> handler = handlerSupplier.get();
                try {
                    handler.setup();
                    final ConsumerIterator<byte[],byte[]> iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        final byte[] message = iterator.next().message();
                        DecoderFactory.get().binaryDecoder(message, decoder);
                        handler.handle(reader.read(null, decoder));
                    }
                } catch(Exception e) {
                    logger.warn("Exception in event handler. Re-scheduling.", e);
                    scheduleReader(stream, decoder, reader);
                }
                try {
                    handler.shutdown();
                } catch(Exception e) {
                    logger.warn("Exception in event handler shutdown.", e);
                }
            }
        });
    }

    private static ConsumerConfig config(
            final String zookeeper,
            final String groupId,
            final long zookeeperSessionTimeoutMs,
            final long zookeeperSyncTimeMs,
            final long autoCommitIntervalMs) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", Long.toString(zookeeperSessionTimeoutMs));
        props.put("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
        props.put("auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));
        return new ConsumerConfig(props);
    }

    private static ThreadFactory createThreadFactory(final ThreadGroup group, final String nameFormat) {
        return new ThreadFactoryBuilder()
        .setNameFormat(nameFormat)
        .setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(group, runnable);
            }
        })
        .build();
    }

    /**
     * Handler for deserialized messages arriving on a Kafka queue. Apart from
     * the {@link EventHandler#handle(Object)} method, this interface has two
     * lifecycle methods. {@link EventHandler#setup()} is called by the consumer
     * thread prior to passing any events to the event handler.
     * {@link EventHandler#handle(Object)} for each received message on a given
     * thread. Finally, {@link EventHandler#shutdown()} is called to allow the
     * event handler to clean up any resources associated with it.
     * <p>
     * In case either {@link EventHandler#setup()} or
     * {@link EventHandler#handle(Object)} throw any uncaught exception, the
     * consumer thread will discard this event handler instance (after
     * attempting to call {@link EventHandler#shutdown()}) and request a new
     * instance from the provided {@link Supplier}. This retry mechanism is
     * infinite.
     *
     * @param <T>
     *            The class that was generated from the Avro schema used for
     *            serialization.
     */
    public static interface EventHandler<T> {
        /**
         * Handle a single incoming message.
         * @param event The deserialized Avro data.
         * @throws Exception In case of failure.
         */
        public void handle(T event) throws Exception;

        /**
         * Setup this event handler.
         * @throws Exception In case of failure.
         */
        public void setup() throws Exception;
        /**
         * Shutdown this event handler.
         * @throws Exception In case of failure.
         */
        public void shutdown() throws Exception;
    }

    /**
     * Simple form of the {@link EventHandler} without lifecycle methods. This
     * is useful for consumers that do not need any setup or teardown associated
     * with event handlers. More over, this allows consumers written in Java 8 to
     * implement event handlers using lambda syntax.
     * <p>
     * In case {@link EventHandler#handle(Object)} throws any uncaught
     * exception, the consumer thread will discard this event handler instance
     * and request a new instance from the provided {@link Supplier}. This retry
     * mechanism is infinite.
     *
     * @param <T>
     *            The class that was generated from the Avro schema used for
     *            serialization.
     */
    public interface SimpleEventHandler<T> {
        /**
         * Handle a single incoming message.
         * @param event The deserialized Avro data.
         * @throws Exception In case of failure.
         */
        public void handle(T event) throws Exception;
    }
}
