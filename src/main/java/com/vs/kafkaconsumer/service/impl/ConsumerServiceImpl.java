package com.vs.kafkaconsumer.service.impl;

import com.vs.kafkaconsumer.dto.ConsumerDto;
import com.vs.kafkaconsumer.exception.NotFoundException;
import com.vs.kafkaconsumer.service.ConsumerService;
import com.vs.kafkaconsumer.worker.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class ConsumerServiceImpl implements ConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private final ExecutorService executor;
    private final Map<String, Map<Integer, Consumer>> consumerRegistry;

    private int baseId = 0;

    @Value("${kafka.defaultServer}")
    private String defaultServer;

    @Value("${kafka.defaultTopic}")
    private String defaultTopic;

    public ConsumerServiceImpl() {
        this.executor = Executors.newCachedThreadPool();
        this.consumerRegistry = new HashMap<>();
    }

    @Override
    public Set<String> getConsumerGroups() {
        return consumerRegistry.keySet();
    }

    @Override
    public List<ConsumerDto> getConsumers() {
        List<ConsumerDto> consumerDtos = new ArrayList<>();
        for (String groupId: consumerRegistry.keySet()) {
            List consumersGroup = consumerRegistry.get(groupId).values().stream().map(this::toDto).collect(Collectors.toList());
            consumerDtos.addAll(consumersGroup);

        }
        return consumerDtos;
    }

    @Override
    public List<ConsumerDto> getConsumers(String groupId) {
        if (!consumerRegistry.containsKey(groupId)) {
            return Collections.emptyList();
        }
        return consumerRegistry.get(groupId).values().stream().map(this::toDto).collect(Collectors.toList());
    }

    @Override
    public ConsumerDto addConsumer(String group, List<String> topics) {
        return addConsumer(group, topics, defaultServer);
    }

    @Override
    public ConsumerDto removeConsumer(String groupId, Integer id) {
        if (!consumerRegistry.containsKey(groupId)) {
            LOGGER.error("Failed to find group {}", groupId);
            throw new NotFoundException(groupId, "Group");
        }

        Map<Integer, Consumer> group = consumerRegistry.get(groupId);
        if (!group.containsKey(id)) {
            LOGGER.error("Failed to find consumer with id {}", id);
            throw new NotFoundException(id.toString(), "Consumer");
        }

        Consumer consumer = group.get(id);
        LOGGER.info("Shutting down consumer: {}...", consumer.getId());
        consumer.shutdown();
        group.remove(id);
        LOGGER.info("Removed consumer: {}!", consumer.getId());
        return toDto(consumer);
    }

    private ConsumerDto toDto(Consumer consumer) {
        return new ConsumerDto(consumer.getId(), consumer.getGroupId(), consumer.getTopics());
    }

    private ConsumerDto addConsumer(String groupId, List<String> topics, String server) {
        // Generate unique id
        int id = generateId();
        // Create runnable instance
        Consumer consumer = new Consumer(id, groupId, topics, server);
        // Save to registry
        putIntoRegistry(groupId, id, consumer);
        // Submit runnable to executor service
        executor.submit(consumer);
        return toDto(consumer);
    }

    private int generateId() {
        return ++baseId;
    }

    private void putIntoRegistry(String groupId, int id, Consumer consumer) {
        if (!consumerRegistry.containsKey(groupId)) {
            consumerRegistry.put(groupId, new HashMap<>());
        }
        consumerRegistry.get(groupId).put(id, consumer);
    }

    @PreDestroy
    public void destroy() {
        LOGGER.info("@PreDestroy callback triggered!");

        for (String groupId: consumerRegistry.keySet()) {
            Map<Integer, Consumer> consumers = consumerRegistry.get(groupId);

            LOGGER.info("Removing consumer group {}...", groupId);
            Iterator<Map.Entry<Integer, Consumer>> it = consumers.entrySet().iterator();
            while (it.hasNext()) {
                Consumer consumer = it.next().getValue();
                LOGGER.info("Shutting down consumer: {}...", consumer.getId());
                consumer.shutdown();
                it.remove(); // avoids a ConcurrentModificationException
            }
        }

        consumerRegistry.clear();

        LOGGER.info("Shutting down executor...");
        executor.shutdown();

        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted! {}", e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Failed to shutdown executor! {}", e.getMessage());
        }

        LOGGER.info("Executor has been shut down.");
    }

}
