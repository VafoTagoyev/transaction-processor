package com.example.txprocessor.unit;

import com.example.txprocessor.logging.LogMasking;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogMaskingTest {

    @Test
    @DisplayName("Only the last four characters survive masking")
    void keepsOnlyTheSuffix() {
        assertThat(LogMasking.mask("4276380012345678")).isEqualTo("************5678");
        assertThat(LogMasking.mask("100001")).isEqualTo("**0001");
    }

    @Test
    @DisplayName("Short values are masked completely rather than revealed")
    void shortValuesAreFullyMasked() {
        assertThat(LogMasking.mask("1234")).isEqualTo("****");
        assertThat(LogMasking.mask("7")).isEqualTo("****");
        assertThat(LogMasking.mask("")).isEqualTo("****");
    }

    @Test
    @DisplayName("Null stays null so log lines read naturally")
    void nullIsPreserved() {
        assertThat(LogMasking.mask(null)).isNull();
    }
}
