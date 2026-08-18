package com.example.txprocessor.processing;

import com.example.txprocessor.domain.OperationType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Commission rules, verbatim from the assignment (section 6, items 21-22):
 *
 * <pre>
 *   INTERNAL                          -> 0 %
 *   EXTERNAL, amount &lt;  1 000 000  -> 1 %
 *   EXTERNAL, amount &gt;= 1 000 000  -> 0.5 %
 * </pre>
 *
 * The boundary is inclusive on the <em>lower</em> rate: exactly 1 000 000 pays 0.5 %.
 * All arithmetic is BigDecimal; a double would already be wrong at this scale of money.
 */
@Component
public class CommissionCalculator {

    /** Amounts from this value upwards get the reduced rate. The comparison is >= . */
    public static final BigDecimal LARGE_AMOUNT_THRESHOLD = new BigDecimal("1000000");

    static final BigDecimal RATE_INTERNAL = BigDecimal.ZERO;
    static final BigDecimal RATE_EXTERNAL_STANDARD = new BigDecimal("0.01");
    static final BigDecimal RATE_EXTERNAL_LARGE = new BigDecimal("0.005");

    /** processed_transactions.commission is NUMERIC(18,2); round once, here, deterministically. */
    private static final int MONEY_SCALE = 2;
    private static final RoundingMode MONEY_ROUNDING = RoundingMode.HALF_UP;

    public BigDecimal calculate(OperationType operationType, BigDecimal amount) {
        BigDecimal rate = rateFor(operationType, amount);
        return amount.multiply(rate).setScale(MONEY_SCALE, MONEY_ROUNDING);
    }

    /**
     * The rate that applies to this operation and amount, before it is multiplied out.
     * Public so the boundary rule can be asserted directly rather than inferred from a
     * rounded commission figure.
     */
    public BigDecimal rateFor(OperationType operationType, BigDecimal amount) {
        if (operationType == OperationType.INTERNAL) {
            return RATE_INTERNAL;
        }
        return amount.compareTo(LARGE_AMOUNT_THRESHOLD) >= 0
                ? RATE_EXTERNAL_LARGE
                : RATE_EXTERNAL_STANDARD;
    }
}
