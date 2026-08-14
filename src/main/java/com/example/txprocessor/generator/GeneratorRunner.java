package com.example.txprocessor.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Entry point of the generator. Runs when {@code generator.enabled=true}, which the compose
 * "generator" service sets; the processing pipeline is disabled in that mode so the generator
 * never competes with itself.
 */
@Component
@ConditionalOnProperty(prefix = "generator", name = "enabled", havingValue = "true")
public class GeneratorRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(GeneratorRunner.class);

    private final ReferenceDataGenerator referenceDataGenerator;
    private final TransactionGenerator transactionGenerator;
    private final GeneratorProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ApplicationContext applicationContext;

    public GeneratorRunner(ReferenceDataGenerator referenceDataGenerator,
                           TransactionGenerator transactionGenerator,
                           GeneratorProperties properties,
                           JdbcTemplate jdbcTemplate,
                           ApplicationContext applicationContext) {
        this.referenceDataGenerator = referenceDataGenerator;
        this.transactionGenerator = transactionGenerator;
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.applicationContext = applicationContext;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Generating dataset: {} transactions, {} cards, {} terminals "
                        + "(internal={}%, card-miss={}%, terminal-miss={}%)",
                properties.getTransactions(), properties.getCards(), properties.getTerminals(),
                properties.getInternalRatio() * 100, properties.getCardMissRatio() * 100,
                properties.getTerminalMissRatio() * 100);

        if (properties.isTruncateFirst()) {
            // RESTART IDENTITY so external ids and primary keys line up between runs, making
            // successive performance measurements directly comparable.
            jdbcTemplate.execute("""
                    TRUNCATE TABLE processed_transactions, account_statistics, outbox_events, transactions
                    RESTART IDENTITY
                    """);
            log.info("Truncated existing data");
        }

        referenceDataGenerator.generate(properties);
        transactionGenerator.generate(properties);

        log.info("Generation complete");

        if (properties.isExitAfterRun()) {
            System.exit(org.springframework.boot.SpringApplication.exit(applicationContext, () -> 0));
        }
    }
}
