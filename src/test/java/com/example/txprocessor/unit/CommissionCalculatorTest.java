package com.example.txprocessor.unit;

import com.example.txprocessor.domain.OperationType;
import com.example.txprocessor.processing.CommissionCalculator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class CommissionCalculatorTest {

    private final CommissionCalculator calculator = new CommissionCalculator();

    @ParameterizedTest(name = "INTERNAL amount {0} -> commission 0")
    @ValueSource(strings = {"0", "1", "999999.99", "1000000.00", "1000000.01", "5000000"})
    @DisplayName("INTERNAL never charges commission, on either side of the threshold")
    void internalIsAlwaysFree(String amount) {
        assertThat(calculator.calculate(OperationType.INTERNAL, new BigDecimal(amount)))
                .isEqualByComparingTo("0.00");
    }

    @ParameterizedTest(name = "EXTERNAL {0} -> {1}")
    @CsvSource({
            // just below the boundary: 1%
            "0.00,        0.00",
            "1.00,        0.01",
            "100.00,      1.00",
            "999999.99,   10000.00",   // 9999.9999 rounded HALF_UP
            // exactly on the boundary: the reduced rate applies, because the rule is >= 1 000 000
            "1000000.00,  5000.00",
            // above the boundary: 0.5%
            "1000000.01,  5000.00",
            "2000000.00,  10000.00",
            "5000000.00,  25000.00"
    })
    @DisplayName("EXTERNAL uses 1% below 1 000 000 and 0.5% from 1 000 000 upwards")
    void externalRates(String amount, String expectedCommission) {
        assertThat(calculator.calculate(OperationType.EXTERNAL, new BigDecimal(amount)))
                .isEqualByComparingTo(expectedCommission);
    }

    @Test
    @DisplayName("THE boundary case: 1 000 000 exactly is charged at the lower rate, not the higher one")
    void exactlyOneMillionUsesTheReducedRate() {
        BigDecimal justBelow = new BigDecimal("999999.99");
        BigDecimal exactly = new BigDecimal("1000000.00");

        BigDecimal belowCommission = calculator.calculate(OperationType.EXTERNAL, justBelow);
        BigDecimal atCommission = calculator.calculate(OperationType.EXTERNAL, exactly);

        assertThat(calculator.rateFor(OperationType.EXTERNAL, justBelow)).isEqualByComparingTo("0.01");
        assertThat(calculator.rateFor(OperationType.EXTERNAL, exactly)).isEqualByComparingTo("0.005");
        // One cent more principal, half the commission - the discontinuity is intentional.
        assertThat(atCommission).isLessThan(belowCommission);
    }

    @Test
    @DisplayName("Commission is always scaled to 2 decimals, matching NUMERIC(18,2)")
    void commissionIsRoundedToMoneyScale() {
        assertThat(calculator.calculate(OperationType.EXTERNAL, new BigDecimal("333.33")).scale()).isEqualTo(2);
        assertThat(calculator.calculate(OperationType.EXTERNAL, new BigDecimal("333.33")))
                .isEqualByComparingTo("3.33");
        // 0.005 * 1234567.89 = 6172.83945 -> HALF_UP -> 6172.84
        assertThat(calculator.calculate(OperationType.EXTERNAL, new BigDecimal("1234567.89")))
                .isEqualByComparingTo("6172.84");
    }

    @Test
    @DisplayName("Very large amounts do not lose precision (BigDecimal, not double)")
    void largeAmountsKeepPrecision() {
        BigDecimal huge = new BigDecimal("999999999999.99");
        assertThat(calculator.calculate(OperationType.EXTERNAL, huge)).isEqualByComparingTo("5000000000.00");
    }
}
