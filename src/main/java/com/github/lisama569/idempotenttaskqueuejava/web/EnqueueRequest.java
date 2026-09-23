package com.github.lisama569.idempotenttaskqueuejava.web;

/** 投递请求体。 */
public record EnqueueRequest(String businessKey, String type, String payload) {
}
