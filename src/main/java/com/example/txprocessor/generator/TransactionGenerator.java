package com.example.txprocessor.generator;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * Bulk-loads the transactions table.
 *
 * <p>Uses PostgreSQL's COPY protocol rather than INSERT batches: COPY skips per-row statement
 * parsing and planning entirely, which is the difference between minutes and tens of minutes for
 * a million rows on a laptop. Rows are streamed in chunks so heap usage stays flat regardless of
 * the requested volume.
 *
 * <p>The dataset is built to exercise the whole error surface, not just the happy path:
 * <ul>
 *   <li>~80% of transactions pair a card and a terminal from the same bank (INTERNAL);</li>
 *   <li>~1% reference a card id that was never written to the cache;</li>
 *   <li>~1% reference a terminal id that was never written to the cache;</li>
 *   <li>a configurable share of amounts sits exactly on the 1 000 000 commission boundary.</li>
 * </ul>
 * The seed is fixed, so the same configuration produces the same dataset on every machine and
 * performance runs are comparable.
 */
@Component
public class TransactionGenerator {

    private static final Logger log = LoggerFactory.getLogger(TransactionGenerator.class);

    // next_attempt_at and created_at are intentionally absent: COPY does not evaluate
    // expressions, so the column DEFAULT now() is what fills them.
    private static final String COPY_COMMAND = """
            COPY transactions (external_id, card_id, terminal_id, amount, currency,
                               transaction_type, status, retry_count)
            FROM STDIN WITH (FORMAT csv)
            """;

    private static final String MISSING_CARD_ID = "999999999";
    private static final String MISSING_TERMINAL_ID = "888888888";
    private static final String CURRENCY = "UZS";
    private static final String[] TRANSACTION_TYPES = {"PURCHASE", "WITHDRAWAL", "TRANSFER", "PAYMENT"};

    private static final BigDecimal MIN_AMOUNT = new BigDecimal("100");
    private static final BigDecimal MAX_AMOUNT = new BigDecimal("5000000");
    private static final BigDecimal BOUNDARY_AMOUNT = new BigDecimal("1000000.00");

    private final JdbcTemplate jdbcTemplate;

    public TransactionGenerator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void generate(GeneratorProperties properties) {
        long start = System.nanoTime();
        Random random = new Random(properties.getSeed());
        long remaining = properties.getTransactions();
        long generated = 0;

        while (remaining > 0) {
            int chunk = (int) Math.min(properties.getBatchSize(), remaining);
            String csv = buildChunk(chunk, generated, random, properties);
            copyChunk(csv);
            generated += chunk;
            remaining -= chunk;
            if (generated % (properties.getBatchSize() * 4L) == 0 || remaining == 0) {
                log.info("Generated {}/{} transactions", generated, properties.getTransactions());
            }
        }

        jdbcTemplate.execute("ANALYZE transactions");
        log.info("Generated {} transactions in {} ms",
                generated, (System.nanoTime() - start) / 1_000_000);
    }

    private String buildChunk(int rows, long startIndex, Random random, GeneratorProperties properties) {
        StringBuilder csv = new StringBuilder(rows * 90);
        for (int i = 0; i < rows; i++) {
            long index = startIndex + i;
            long cardIndex = random.nextInt(properties.getCards());

            long terminalIndex = random.nextDouble() < properties.getInternalRatio()
                    ? sameBankTerminal(cardIndex, random, properties.getTerminals())
                    : differentBankTerminal(cardIndex, random, properties.getTerminals());

            String cardId = random.nextDouble() < properties.getCardMissRatio()
                    ? MISSING_CARD_ID
                    : ReferenceDataGenerator.cardId(cardIndex);
            String terminalId = random.nextDouble() < properties.getTerminalMissRatio()
                    ? MISSING_TERMINAL_ID
                    : ReferenceDataGenerator.terminalId(terminalIndex);

            csv.append("TX-").append(String.format("%012d", index)).append(',')
                    .append(cardId).append(',')
                    .append(terminalId).append(',')
                    .append(amount(random, properties)).append(',')
                    .append(CURRENCY).append(',')
                    .append(TRANSACTION_TYPES[random.nextInt(TRANSACTION_TYPES.length)]).append(',')
                    .append("NEW,0\n");
        }
        return csv.toString();
    }

    /** Terminal whose round-robin bank code equals the card's. */
    private long sameBankTerminal(long cardIndex, Random random, int terminals) {
        int banks = ReferenceDataGenerator.BANK_CODES.length;
        int target = (int) Math.floorMod(cardIndex, banks);
        int slots = Math.max(1, terminals / banks);
        long candidate = (long) random.nextInt(slots) * banks + target;
        return Math.min(candidate, terminals - 1L);
    }

    /** Terminal whose round-robin bank code differs from the card's. */
    private long differentBankTerminal(long cardIndex, Random random, int terminals) {
        int banks = ReferenceDataGenerator.BANK_CODES.length;
        int cardBank = (int) Math.floorMod(cardIndex, banks);
        int target = (cardBank + 1 + random.nextInt(banks - 1)) % banks;
        int slots = Math.max(1, terminals / banks);
        long candidate = (long) random.nextInt(slots) * banks + target;
        return Math.min(candidate, terminals - 1L);
    }

    private BigDecimal amount(Random random, GeneratorProperties properties) {
        if (random.nextDouble() < properties.getBoundaryAmountRatio()) {
            return BOUNDARY_AMOUNT;
        }
        // Log-uniform so small and very large amounts are both well represented, which puts a
        // meaningful share of transactions on either side of the 1 000 000 commission threshold.
        double logMin = Math.log(MIN_AMOUNT.doubleValue());
        double logMax = Math.log(MAX_AMOUNT.doubleValue());
        double value = Math.exp(logMin + random.nextDouble() * (logMax - logMin));
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    private void copyChunk(String csv) {
        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
            try {
                copyManager.copyIn(COPY_COMMAND, new StringReader(csv));
            } catch (IOException e) {
                throw new IllegalStateException("COPY into transactions failed", e);
            }
            return null;
        });
    }
}
