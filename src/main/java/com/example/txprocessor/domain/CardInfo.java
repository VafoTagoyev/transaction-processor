package com.example.txprocessor.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Reference data under Redis key {@code card:{cardId}}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CardInfo(String clientId, String account, String productId, String bankCode) {
}
