package com.example.txprocessor.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Real PostgreSQL and real Valkey, started once for the whole test run.
 *
 * <p>The concurrency and recovery guarantees under test are properties of PostgreSQL's locking
 * and of the exact SQL used - SKIP LOCKED, ON CONFLICT, the fenced UPDATE. An in-memory database
 * or a mock would not exercise any of them, so these tests would prove nothing. Everything here
 * is wired by hand rather than through a Spring context, so a single JVM can host several
 * independent "instances" against one database, which is what the multi-instance tests need.
 */
@Testcontainers
public abstract class IntegrationTestBase {

    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("txprocessor")
                    .withUsername("txprocessor")
                    .withPassword("txprocessor")
                    .withCommand("postgres", "-c", "max_connections=200", "-c", "fsync=off",
                            "-c", "synchronous_commit=off", "-c", "full_page_writes=off");

    protected static final GenericContainer<?> VALKEY =
            new GenericContainer<>(DockerImageName.parse("valkey/valkey:8-alpine"))
                    .withExposedPorts(6379)
                    .withCommand("valkey-server", "--save", "", "--appendonly", "no")
                    .waitingFor(Wait.forListeningPort());

    protected static HikariDataSource dataSource;
    protected static JdbcTemplate jdbcTemplate;
    protected static PlatformTransactionManager transactionManager;
    protected static LettuceConnectionFactory redisConnectionFactory;
    protected static StringRedisTemplate redisTemplate;
    /**
     * Built the way Spring Boot builds its own, not with {@code new ObjectMapper()}.
     *
     * <p>This mapper is handed to {@code ResultPersistenceService} through the harness, standing in
     * for the auto-configured bean the application receives at runtime. A bare mapper registers no
     * modules, so it cannot serialise the {@link java.time.Instant} on
     * {@code TransactionProcessedEvent} and every outbox write fails - a failure the application
     * itself never has. {@code Jackson2ObjectMapperBuilder} is what Boot's JacksonAutoConfiguration
     * uses: it registers the well-known modules on the classpath (JavaTimeModule among them) and
     * disables WRITE_DATES_AS_TIMESTAMPS, so timestamps land in the payload as the same ISO-8601
     * strings production writes rather than as epoch numbers.
     */
    protected static final ObjectMapper OBJECT_MAPPER = Jackson2ObjectMapperBuilder.json().build();

    @BeforeAll
    static void startInfrastructure() {
        if (dataSource != null) {
            return;
        }
        POSTGRES.start();
        VALKEY.start();

        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(60);
        dataSource.setPoolName("it-pool");

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionManager = new DataSourceTransactionManager(dataSource);

        RedisStandaloneConfiguration redisConfiguration =
                new RedisStandaloneConfiguration(VALKEY.getHost(), VALKEY.getMappedPort(6379));
        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofSeconds(2))
                .build();
        redisConnectionFactory = new LettuceConnectionFactory(redisConfiguration, clientConfiguration);
        redisConnectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(redisConnectionFactory);
    }

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("""
                TRUNCATE TABLE processed_transactions, account_statistics, outbox_events, transactions
                RESTART IDENTITY
                """);
        try (var connection = redisConnectionFactory.getConnection()) {
            connection.serverCommands().flushAll();
        }
    }

    protected TestFixtures fixtures() {
        return new TestFixtures(jdbcTemplate, redisTemplate, OBJECT_MAPPER);
    }
}
