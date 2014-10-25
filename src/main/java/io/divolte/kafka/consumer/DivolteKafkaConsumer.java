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

    @SuppressWarnings("PMD.AvoidThreadGroup")
    public DivolteKafkaConsumer(
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

    public DivolteKafkaConsumer(
            final String topic,
            final String zookeeper,
            final String groupId,
            final int numThreads,
            final Supplier<EventHandler<T>> handlerSupplier,
            final Schema schema) {
        this(topic, zookeeper, groupId, numThreads, handlerSupplier, schema, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_SYNC_TIMEOUT, DEFAULT_AUTO_COMMIT_INTERVAL);
    }

    public void startConsumer() {
        ImmutableMap<String, Integer> threadsPerTopicMap = ImmutableMap.of(Objects.requireNonNull(topic), numThreads);
        for(final KafkaStream<byte[], byte[]> stream : consumer.createMessageStreams(threadsPerTopicMap).get(topic)) {
            final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
            final SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
            scheduleReader(stream, decoder, reader);
        }
    }

    public void shutdownConsumer() {
        consumer.shutdown();
        executorService.shutdown();
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

    public interface EventHandler<T> {
        public void handle(T event) throws Exception;
        public void setup() throws Exception;
        public void shutdown() throws Exception;
    }
}
