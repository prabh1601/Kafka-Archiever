//package com.spr.messaging.kafka.raw.consumer;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import com.google.gson.Gson;
//import com.spr.atlas.counter.SprAtlasCounters;
//import com.spr.core.config.ServerConfigurationMissingException;
//import com.spr.core.logger.Logger;
//import com.spr.core.logger.LoggerFactory;
//import com.spr.core.sprapp.SprAppLifecycle;
//import com.spr.exceptions.unchecked.MultiException;
//import com.spr.messaging.kafka.KafkaPoisonService;
//import com.spr.messaging.kafka.raw.KafkaPoisonMessage;
//import com.spr.messaging.kafka.raw.consumer.domain.RawKafkaConsumerKey;
//import com.spr.ops.BaseContextTransferableThread;
//import com.spr.resilence.MicrometerRegistry;
//import com.spr.utils.SprinklrCollectionUtils;
//import com.spr.utils.TPSCounter;
//import com.spr.utils.ThreadUtil;
//import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
//
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.TopicPartition;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * User: abhay
// * Date: 6/9/17
// * Time: 2:42 PM
// * <p>
// * Any one should be able to use this to read data from multiple consumers. Streamer is stateful
// * You initialize with the number of consumers you want to start for a JVM and a type.
// * For each consumer it will push data to a queue.
// * Anyone should be able to pass on a processor to this data.
// * Process will be called with batches from the service.
// **/
//public abstract class RawKafkaStreamer<K, V> implements ConsumerRebalanceListener {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(RawKafkaStreamer.class);
//    // Max throughput per sec per consumer would be number of records in one poll -> say 500 * 100 = 50K/s
//    // To scale up increase the number of partitions and the consumer threads.
//    private static final int WAIT_TIME_IN_MILLIS = 10;
//    private static final int MAX_THREAD_NAME_LENGTH = 100;
//    private static final int SLEEP_INTERVAL_FOR_DELAYED_PACKET = 50; // 200ms
//
//    protected final KafkaConsumerService<K, V> kafkaConsumerService;
//    private final String streamerKey;
//    private final KafkaPoisonService poisonService;
//    private final String subscriptionTopic;
//
//    private TPSCounter incomingTPS;
//
//    // 0 -> New
//    // 1 -> Running
//    // 2 -> Stopping
//    // 3 -> Terminated
//    private final AtomicInteger state = new AtomicInteger(0);
//
//    private final List<Consumer<K, V>> consumers = Lists.newArrayList();
//    private final AtomicInteger runningConsumers = new AtomicInteger(0);
//    private final Set<TopicPartition> assignedPartitions = Sets.newConcurrentHashSet();
//
//    protected final RawKafkaConsumerKey rawKafkaConsumerKey;
//    private int numberOfConsumers;
//
//    public RawKafkaStreamer(RawKafkaConsumerKey rawKafkaConsumerKey, KafkaConsumerService<K, V> kafkaConsumerService,
//                            StreamerRefreshService streamerRefreshService) {
//        this(rawKafkaConsumerKey, kafkaConsumerService, streamerRefreshService, rawKafkaConsumerKey.getTopicName());
//    }
//
//    public RawKafkaStreamer(RawKafkaConsumerKey rawKafkaConsumerKey, KafkaConsumerService<K, V> kafkaConsumerService,
//                            StreamerRefreshService streamerRefreshService, String subscriptionTopic) {
//        this.rawKafkaConsumerKey = rawKafkaConsumerKey;
//        this.kafkaConsumerService = kafkaConsumerService;
//        this.poisonService = kafkaConsumerService.getPoisonService();
//        this.streamerKey = rawKafkaConsumerKey.getTopicName() + "_" + rawKafkaConsumerKey.getGroup();
//        this.subscriptionTopic = subscriptionTopic;
//        streamerRefreshService.addStreamer(this);
//    }
//
//    public final void start(int numberOfConsumers) {
//        this.numberOfConsumers = numberOfConsumers;
//        final boolean initialize = state.compareAndSet(0, 1);
//        if (initialize) {
//            incomingTPS = new TPSCounter(streamerKey + " - incoming");
//            startConsumers();
//        }
//    }
//
//    private void startConsumers() {
//        try {
//            for (int consumerNumber = 0; consumerNumber < this.numberOfConsumers; consumerNumber++) {
//                Consumer<K, V> consumer = createConsumer(consumerNumber);
//                consumer.setName("CONSUMER_THREAD_" + streamerKey + "_" + consumerNumber);
//                consumer.start();
//
//                synchronized (consumers) {
//                    consumers.add(consumer);
//                }
//            }
//        } catch (Exception e) {
//            LOGGER.error(e.getMessage(), e);
//            state.set(0);
//        }
//    }
//
//    protected Consumer<K, V> createConsumer(int number) {
//        return new Consumer<>(this, number);
//    }
//
//    public abstract void process(ConsumerRecord<K, V> record);
//
//    public int getRunningConsumers() {
//        return runningConsumers.get();
//    }
//
//    public String getStreamerKey() {
//        return streamerKey;
//    }
//
//    public void close() {
//
//    }
//
//    public Set<TopicPartition> assignedPartitions() {
//        Set<TopicPartition> toReturn = new HashSet<>();
//        for (TopicPartition currPartition : assignedPartitions) {
//            if (currPartition.topic().equalsIgnoreCase(subscriptionTopic)) {
//                toReturn.add(currPartition);
//            }
//        }
//        return toReturn;
//    }
//
//    public final int getState() {
//        return state.get();
//    }
//
//    public final boolean isRunning() {
//        return state.get() == 1;
//    }
//
//    public final boolean isStopRequested() {
//        return state.get() == 2 || state.get() == 3;
//    }
//
//    public final void stop() {
//        try {
//            state.set(2);
//            closeConsumers();
//            // Wait for consumers to finish
//            while (runningConsumers.get() != 0) {
//                ThreadUtil.reallySleep(WAIT_TIME_IN_MILLIS);
//            }
//        } finally {
//            state.set(3);
//            close();
//        }
//    }
//
//    public final void refresh() {
//        closeConsumers();
//        startConsumers();
//    }
//
//    private void closeConsumers() {
//        synchronized (consumers) {
//            consumers.forEach(Consumer::close);
//            consumers.clear();
//        }
//    }
//
//    public String getSubscriptionTopic() {
//        return subscriptionTopic;
//    }
//
//    protected KafkaConsumer<K, V> getKafkaConsumer() {
//        KafkaConsumer<K, V> kafkaConsumer = kafkaConsumerService.createKafkaConsumer(rawKafkaConsumerKey);
//        kafkaConsumer.subscribe(Collections.singletonList(subscriptionTopic), this);
//        return kafkaConsumer;
//    }
//
//    @Override
//    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//        assignedPartitions.removeAll(collection);
//        onRebalance();
//    }
//
//    @Override
//    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//        assignedPartitions.addAll(collection);
//        onRebalance();
//    }
//
//    protected long delayInMillis() {
//        return -1;
//    }
//
//    protected void onRebalance() {
//
//    }
//
//    protected long getPollTimeoutMS() {
//        return TimeUnit.SECONDS.toMillis(10);
//    }
//
//    protected boolean commitAsync() {
//        return true;
//    }
//
//    protected boolean batchSupported() {
//        return false;
//    }
//
//    protected void consumeBatch(Iterable<ConsumerRecord<K, V>> records) {
//        throw new UnsupportedOperationException("Consume batch not supported");
//    }
//
//    protected static class Consumer<Key, Value> extends BaseContextTransferableThread {
//
//        private final RawKafkaStreamer<Key, Value> rawKafkaStreamer;
//        private final AtomicBoolean isClosed = new AtomicBoolean(false);
//        private final int consumerNumber;
//
//        protected Consumer(RawKafkaStreamer<Key, Value> rawKafkaStreamer, int consumerNumber) {
//            this.rawKafkaStreamer = rawKafkaStreamer;
//            this.consumerNumber = consumerNumber;
//        }
//
//        @Override
//        public void doRun() {
//            rawKafkaStreamer.runningConsumers.incrementAndGet();
//            try {
//                KafkaConsumer<Key, Value> kafkaConsumer = null;
//                while (isRunning()) {
//                    try {
//                        kafkaConsumer = initializeConsumerIfNeeded(kafkaConsumer);
//                        if (kafkaConsumer == null) {
//                            return;
//                        }
//
//                        final ConsumerRecords<Key, Value> records = kafkaConsumer.poll(Duration.ofMillis(rawKafkaStreamer.getPollTimeoutMS()));
//
//                        if (records != null && !records.isEmpty()) {
//                            log(records);
//                            if (rawKafkaStreamer.batchSupported()) {
//                                rawKafkaStreamer.consumeBatch(records);
//                            } else {
//                                List<Exception> exceptions = new ArrayList<>();
//                                StopStreamerSignalException stopStreamerSignal = null;
//                                for (ConsumerRecord<Key, Value> record : records) {
//                                    try {
//                                        SprAtlasCounters.incrSampledCounter(record.topic() + ".record." + record.partition());
//                                        consumeInternal(kafkaConsumer, record, exceptions);
//                                    } catch (StopStreamerSignalException stopStreamerSignalException) {
//                                        stopStreamerSignal = stopStreamerSignalException;
//                                    }
//                                }
//                                throwOnError(exceptions, stopStreamerSignal);
//                            }
//                        }
//                        if (!rawKafkaStreamer.commitAsync()) {
//                            kafkaConsumer.commitSync();
//                        }
//                    } catch (ServerConfigurationMissingException e) {
//                        doFailureLogging(e);
//                        throw e;
//                    } catch (StopStreamerSignalException e) {
//                        throw e;
//                    } catch (Exception e) {
//                        doFailureLogging(e);
//                    } finally {
//                        if (isClosed.get()) {
//                            try {
//                                if (kafkaConsumer != null) {
//                                    kafkaConsumer.close();
//                                }
//                            } finally {
//                                kafkaConsumer = null;
//                            }
//                        }
//                    }
//                }
//            } finally {
//                int consumerCount = rawKafkaStreamer.runningConsumers.decrementAndGet();
//                LOGGER.error("Terminating consumer, final consumerCount:" + consumerCount);
//            }
//        }
//
//        private void setThreadName(KafkaConsumer<Key, Value> kafkaConsumer) {
//            try {
//                Set<TopicPartition> topicPartitions = kafkaConsumer.assignment();
//                if (SprinklrCollectionUtils.isNotEmpty(topicPartitions)) {
//                    StringBuilder builder =
//                            new StringBuilder("CONSUMER_THREAD-" + rawKafkaStreamer.rawKafkaConsumerKey.getTopicName() + "-" + consumerNumber);
//                    for (TopicPartition topicPartition : topicPartitions) {
//                        builder.append("_").append(topicPartition.partition());
//                    }
//                    if (builder.length() > MAX_THREAD_NAME_LENGTH) {
//                        builder = new StringBuilder(builder.substring(0, MAX_THREAD_NAME_LENGTH));
//                    }
//                    Thread.currentThread().setName(builder.toString());
//                }
//            } catch (Exception e) {
//                LOGGER.error(e.getMessage(), e);
//            }
//        }
//
//        private void consumeInternal(KafkaConsumer<Key, Value> consumer, ConsumerRecord<Key, Value> record, List<Exception> exceptions) {
//            if (rawKafkaStreamer.delayInMillis() != -1) { // check if delayed
//                if (record.timestamp() != 0L) {
//                    List<ConsumerRecord<Key, Value>> additionalRecords = delayIfNecessary(consumer, record, rawKafkaStreamer.delayInMillis());
//                    consumeRecord(consumer, record, exceptions);
//                    if (SprinklrCollectionUtils.isNotEmpty(additionalRecords)) {
//                        LOGGER.error("[FATAL] records in paused stream. topic:" + rawKafkaStreamer.subscriptionTopic);
//                        additionalRecords.forEach((t) -> consumeRecord(consumer, t, exceptions)); // these records should not come ideally.
//                    }
//                    return;
//                }
//            }
//            consumeRecord(consumer, record, exceptions);
//        }
//
//        protected void consumeRecord(KafkaConsumer<Key, Value> consumer, ConsumerRecord<Key, Value> record, List<Exception> exceptions) {
//            try {
//                if (KafkaPoisonService.isPoison(record)) {
//                    KafkaPoisonMessage poison = new Gson().fromJson((String) record.value(), KafkaPoisonMessage.class);
//                    rawKafkaStreamer.poisonService.process(poison.getPartnerId(), poison.getMsgPartitionNumber(), poison.getMongoId());
//                } else {
//                    rawKafkaStreamer.process(record);
//                }
//            } catch (StopStreamerSignalException | ServerConfigurationMissingException e) {
//                throw e;
//            } catch (Exception e) {
//                exceptions.add(e);
//            }
//        }
//
//        private List<ConsumerRecord<Key, Value>> delayIfNecessary(KafkaConsumer<Key, Value> consumer, ConsumerRecord<Key, Value> record, long delay) {
//            if (delay + record.timestamp() > System.currentTimeMillis()) {
//                try {
//                    return pauseConsumer(consumer, record, delay);
//                } finally {
//                    //Resume all the partitions for the consumer as after this step we dont want any paused partition
//                    consumer.resume(consumer.paused());
//                }
//            }
//            return Collections.emptyList();
//        }
//
//        private List<ConsumerRecord<Key, Value>> pauseConsumer(KafkaConsumer<Key, Value> consumer, ConsumerRecord<Key, Value> record, long delay) {
//            List<ConsumerRecord<Key, Value>> additionalRecords = new ArrayList<>();
//            long timeToWait = delay - (System.currentTimeMillis() - record.timestamp());
//
//            if (timeToWait > 0) {
//                SprAtlasCounters.getInstance().incrementSampledCounter("standalone.delayed.event");
//
//                //Pause all the partitions for the consumer as after this step we dont want any consumption for this consumer until the wait time
//                consumer.pause(consumer.assignment());
//
//                while (timeToWait > 0) {
//                    ThreadUtil.reallySleep(SLEEP_INTERVAL_FOR_DELAYED_PACKET);
//
//                    //This is used as a heartbeat so that broker dont think that this consumer is dead.
//                    ConsumerRecords<Key, Value> recordsThatShouldNotCome = consumer.poll(Duration.ZERO);
//
//                    if (!recordsThatShouldNotCome.isEmpty()) {
//                        //Some partition which is assigned to this consumer is not paused. We need to pause that, all partitions for a consumer should be paused.
//                        consumer.pause(consumer.assignment());
//
//                        LOGGER.error("[KAFKA-FATAL] recordsThatShouldNotCome are coming even after paused paritions.");
//                        for (ConsumerRecord<Key, Value> recordThatShouldNotCome : recordsThatShouldNotCome) {
//                            final List<ConsumerRecord<Key, Value>> consumerRecords = pauseConsumer(consumer, recordThatShouldNotCome, delay);
//                            additionalRecords.add(recordThatShouldNotCome);
//                            additionalRecords.addAll(consumerRecords);
//                            SprAtlasCounters.getInstance().incrementSampledCounter("standalone.record.that.should.not.come");
//                        }
//                    }
//                    timeToWait = delay - (System.currentTimeMillis() - record.timestamp());
//                }
//            }
//            return additionalRecords;
//        }
//
//        private boolean isRunning() {
//            return SprAppLifecycle.isRunning() && rawKafkaStreamer.state.get() == 1;
//        }
//
//        private void doFailureLogging(Throwable t) {
//            LOGGER.error("Error in RawKafkaStreamerThread [" + rawKafkaStreamer.streamerKey + "]", t);
//        }
//
//        public void close() {
//            isClosed.compareAndSet(false, true);
//        }
//
//        private void log(ConsumerRecords<Key, Value> records) {
//            if (LOGGER.isInfoEnabled()) {
//                LOGGER.info("{} polled {} records", Thread.currentThread().getName(), records.count());
//            }
//
//            if (rawKafkaStreamer.incomingTPS != null && records.count() > 0) {
//                rawKafkaStreamer.incomingTPS.increment(records.count());
//            }
//        }
//
//        private void throwOnError(List<Exception> exceptions, StopStreamerSignalException stopStreamerSignal) {
//            if (stopStreamerSignal != null) {
//                throw stopStreamerSignal;
//            }
//            if (SprinklrCollectionUtils.isNotEmpty(exceptions)) {
//                throw new MultiException(exceptions);
//            }
//        }
//
//        private KafkaConsumer<Key, Value> initializeConsumerIfNeeded(KafkaConsumer<Key, Value> kafkaConsumer) {
//            if (kafkaConsumer == null && !isClosed.get()) {
//                final KafkaConsumer<Key, Value> refreshedConsumer = rawKafkaStreamer.getKafkaConsumer();
//                if (refreshedConsumer == null) {
//                    return null;
//                }
//                MicrometerRegistry.bind(new KafkaClientMetrics(refreshedConsumer));
//                kafkaConsumer = refreshedConsumer;
//                setThreadName(kafkaConsumer);
//                isClosed.compareAndSet(true, false);
//            }
//            return kafkaConsumer;
//        }
//    }
//}
