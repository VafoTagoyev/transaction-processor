package com.example.txprocessor.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Reference data under Redis key {@code terminal:{terminalId}}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TerminalInfo(String merchantId, String branchCode, String terminalType, String bankCode) {
}
