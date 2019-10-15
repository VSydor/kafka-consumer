package com.vs.kafkaconsumer.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Order
@RestControllerAdvice
public class ModelExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModelExceptionHandler.class);

    @ResponseBody
    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(INTERNAL_SERVER_ERROR)
    public ErrorResponse internalServerError(RuntimeException e) {
        LOGGER.error(e.getMessage(), e);
        return new ErrorResponse(e.getLocalizedMessage());
    }

    @ResponseBody
    @ResponseStatus(NOT_FOUND)
    @ExceptionHandler(NotFoundException.class)
    public ErrorResponse notFound(NotFoundException e) {
        String entityType = e.getEntityName();
        String defaultMessage = String.format("%s with id '%s' was not found", entityType, e.getId());
        LOGGER.error(defaultMessage);
        return new ErrorResponse(defaultMessage);
    }

}
