package com.vs.kafkaconsumer.service;

import com.vs.kafkaconsumer.dto.ConsumerDto;

import java.util.List;
import java.util.Set;

public interface ConsumerService {

    Set<String> getConsumerGroups();

    List<ConsumerDto> getConsumers();

    List<ConsumerDto> getConsumers(String group);

    ConsumerDto addConsumer(String group, List<String> topics);

    ConsumerDto removeConsumer(String groupId, Integer id);

}
