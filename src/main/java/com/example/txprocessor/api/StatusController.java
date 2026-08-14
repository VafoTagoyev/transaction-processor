package com.example.txprocessor.api;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.TransactionStatus;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.repository.ProcessedTransactionRepository;
import com.example.txprocessor.repository.TransactionClaimRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single endpoint that answers the acceptance-criteria questions directly, so the crash and
 * performance demonstrations can be verified without opening psql.
 */
@RestController
@RequestMapping("/status")
public class StatusController {

    private final TransactionClaimRepository transactionRepository;
    private final ProcessedTransactionRepository processedRepository;
    private final ProcessorMetrics metrics;
    private final ProcessorProperties properties;

    public StatusController(TransactionClaimRepository transactionRepository,
                            ProcessedTransactionRepository processedRepository,
                            ProcessorMetrics metrics,
                            ProcessorProperties properties) {
        this.transactionRepository = transactionRepository;
        this.processedRepository = processedRepository;
        this.metrics = metrics;
        this.properties = properties;
    }

    @GetMapping
    public Map<String, Object> status() {
        Map<String, Object> counts = new LinkedHashMap<>();
        for (TransactionStatus status : TransactionStatus.values()) {
            counts.put(status.name(), transactionRepository.countByStatus(status));
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instanceId", properties.getInstanceId());
        body.put("transactions", counts);
        body.put("processedTransactions", processedRepository.count());
        body.put("duplicateTransactionIds", processedRepository.countDuplicateTransactionIds());
        body.put("stuckProcessing",
                transactionRepository.countStuckProcessing(properties.getProcessingTimeout().toMillis() / 1000.0));
        body.put("localProcessed", metrics.processedCount());
        body.put("localErrors", metrics.errorCount());
        body.put("localRetries", metrics.retryCount());
        body.put("localRecovered", metrics.recoveredCount());
        body.put("localOwnershipLost", metrics.ownershipLostCount());
        body.put("localDuplicateSkipped", metrics.duplicateSkippedCount());
        body.put("localBackpressureEvents", metrics.backpressureCount());
        return body;
    }
}
