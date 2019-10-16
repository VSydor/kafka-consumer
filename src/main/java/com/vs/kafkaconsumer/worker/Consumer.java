package com.vs.kafkaconsumer.worker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Consumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private final int id;
    private final String groupId;
    private final List<String> topics;

    private final KafkaConsumer<String, String> consumer;

    public Consumer(int id, String groupId, List<String> topics, String server) {
        this.id = id;
        this.groupId = groupId;
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(createProperties(this.id, this.groupId, this.topics, server));
    }

    @Override
    public void run() {
        final int giveUp = 10;
        int noRecordsCount = 0;
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

                if (records.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) {
                        LOGGER.info("Gave up after {} tries!", giveUp);
                        break;
                    }

                }

                for (ConsumerRecord<String, String> record : records) {
                    logRecord(record);
                }

                LOGGER.info("Received {} records in total!", records.count());
            }
        } catch (WakeupException e) {
            // Ignore
        } catch (Exception e) {
            LOGGER.error("Consumer error: {}", e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private Properties createProperties(int id, String groupId, List<String> topics, String server) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Using StringDeserializer as a default
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    private void logRecord(ConsumerRecord<String, String> record) {
        Map<String, Object> data = new HashMap<>();
        data.put("partition", record.partition());
        data.put("offset", record.offset());
        data.put("value", record.value());
        LOGGER.info("{}: {}", this.id, data);
    }

    public void shutdown() {
        consumer.wakeup();
    }

    public int getId() {
        return id;
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getGroupId() {
        return this.groupId;
    }

}
