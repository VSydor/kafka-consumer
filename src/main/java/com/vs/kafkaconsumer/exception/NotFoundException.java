package com.vs.kafkaconsumer.exception;


public class NotFoundException extends RuntimeException {

    private final String id;
    private final String entityName;

    public NotFoundException(String id, String entityName) {
        this.id = id;
        this.entityName = entityName;
    }

    public String getId() {
        return id;
    }

    public String getEntityName() {
        return entityName;
    }

}
