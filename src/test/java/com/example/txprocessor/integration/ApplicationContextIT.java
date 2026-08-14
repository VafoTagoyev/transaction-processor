package com.example.txprocessor.integration;

import com.example.txprocessor.TransactionProcessorApplication;
import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.processing.ProcessingPipeline;
import com.example.txprocessor.recovery.LeaseRenewalService;
import com.example.txprocessor.recovery.StaleProcessingRecoveryService;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that the real Spring wiring works: Flyway migrates, every bean resolves, the pipeline
 * starts, actuator answers, and every metric required by the assignment is actually registered.
 */
@Testcontainers
@SpringBootTest(classes = TransactionProcessorApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "processor.instance-id=context-test",
                "processor.workers=2",
                "processor.polling-idle-delay=1s",
                "spring.datasource.hikari.maximum-pool-size=10"
        })
class ApplicationContextIT {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("txprocessor")
                    .withUsername("txprocessor")
                    .withPassword("txprocessor");

    static final GenericContainer<?> VALKEY =
            new GenericContainer<>(DockerImageName.parse("valkey/valkey:8-alpine"))
                    .withExposedPorts(6379)
                    .waitingFor(Wait.forListeningPort());

    static {
        POSTGRES.start();
        VALKEY.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", VALKEY::getHost);
        registry.add("spring.data.redis.port", () -> VALKEY.getMappedPort(6379));
    }

    @LocalServerPort
    int port;

    @Autowired
    ProcessorProperties processorProperties;

    @Autowired
    ProcessingPipeline pipeline;

    @Autowired
    StaleProcessingRecoveryService recoveryService;

    @Autowired
    LeaseRenewalService leaseRenewalService;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("The context starts, Flyway migrates and the pipeline is running")
    void contextStarts() {
        assertThat(processorProperties.getInstanceId()).isEqualTo("context-test");
        assertThat(pipeline.isRunning()).isTrue();
        assertThat(recoveryService).isNotNull();
        assertThat(leaseRenewalService).isNotNull();

        List<String> tables = jdbcTemplate.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", String.class);
        assertThat(tables).contains("transactions", "processed_transactions", "account_statistics", "outbox_events");

        List<String> indexes = jdbcTemplate.queryForList(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'transactions'", String.class);
        assertThat(indexes).contains("idx_transactions_claim", "idx_transactions_stale_processing");
    }

    @Test
    @DisplayName("Every metric named in the assignment is registered")
    void requiredMetricsExist() {
        List<String> registered = meterRegistry.getMeters().stream()
                .map(meter -> meter.getId().getName())
                .toList();

        assertThat(registered).contains(
                "processor.received", "processor.processed", "processor.error",
                "processor.retry", "processor.recovered",
                "processing.duration", "redis.lookup.duration", "db.write.duration",
                "active.workers", "processing.queue.size");
    }

    @Test
    @DisplayName("Health, Prometheus and the status endpoint all answer")
    @SuppressWarnings("unchecked")
    void actuatorEndpointsAnswer() {
        TestRestTemplate rest = new TestRestTemplate();
        String base = "http://localhost:" + port;

        Map<String, Object> health = rest.getForObject(base + "/actuator/health", Map.class);
        assertThat(health).containsEntry("status", "UP");

        String prometheus = rest.getForObject(base + "/actuator/prometheus", String.class);
        assertThat(prometheus)
                .contains("processor_received_total")
                .contains("processor_processed_total")
                .contains("processor_error_total")
                .contains("processor_retry_total")
                .contains("processor_recovered_total")
                .contains("processing_queue_size")
                .contains("active_workers")
                .contains("processing_duration_seconds")
                .contains("redis_lookup_duration_seconds")
                .contains("db_write_duration_seconds");

        Map<String, Object> status = rest.getForObject(base + "/status", Map.class);
        assertThat(status).containsKey("transactions");
        assertThat(status).containsEntry("instanceId", "context-test");
        assertThat(((Number) status.get("duplicateTransactionIds")).longValue()).isZero();
    }
}
