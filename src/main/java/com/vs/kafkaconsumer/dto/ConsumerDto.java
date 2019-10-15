package com.vs.kafkaconsumer.dto;

import java.util.List;

public class ConsumerDto {

    private final Integer id;
    private String groupId;
    private List<String> topics;

    public ConsumerDto(Integer id, String groupId, List<String> topics) {
        this.id = id;
        this.groupId = groupId;
        this.topics = topics;
    }

    public Integer getId() {
        return id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

}
