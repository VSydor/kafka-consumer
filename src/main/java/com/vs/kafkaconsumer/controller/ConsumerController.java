package com.vs.kafkaconsumer.controller;

import com.vs.kafkaconsumer.dto.ConsumerDto;
import com.vs.kafkaconsumer.service.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

@RestController
public class ConsumerController {

    @Autowired
    ConsumerService consumerService;

    @GetMapping("/consumers")
    public List<ConsumerDto> getConsumers() {
        return consumerService.getConsumers();
    }

    @GetMapping("/consumers/groups")
    public Set<String> getConsumerGroups() {
        return consumerService.getConsumerGroups();
    }

    @GetMapping("/consumers/{groupId}")
    public List<ConsumerDto> getConsumers(
            @PathVariable String groupId) {
        return consumerService.getConsumers(groupId);
    }

    @PostMapping("/consumers/{groupId}")
    public ConsumerDto addConsumer(
            @PathVariable String groupId,
            @RequestParam List<String> topics) {
        return consumerService.addConsumer(groupId, topics);
    }

    @DeleteMapping("/consumers/{groupId}/{id}")
    public ConsumerDto removeConsumer(
            @PathVariable String groupId,
            @PathVariable Integer id) {
        return consumerService.removeConsumer(groupId, id);
    }

}
