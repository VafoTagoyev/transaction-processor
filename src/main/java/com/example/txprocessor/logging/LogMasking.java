package com.example.txprocessor.logging;

/**
 * Card and account identifiers are never written to logs in full. The assignment's card ids are
 * surrogate keys rather than PANs, but the processing path is exactly where a real PAN would
 * leak, so the masking rule is applied unconditionally and lives in one place.
 */
public final class LogMasking {

    private static final int VISIBLE_SUFFIX = 4;
    private static final String FULLY_MASKED = "****";

    private LogMasking() {
    }

    public static String mask(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() <= VISIBLE_SUFFIX) {
            return FULLY_MASKED;
        }
        return "*".repeat(value.length() - VISIBLE_SUFFIX) + value.substring(value.length() - VISIBLE_SUFFIX);
    }
}
