package com.example.txprocessor.error;

public class TerminalNotFoundException extends PermanentProcessingException {

    public static final String CODE = "TERMINAL_NOT_FOUND";

    public TerminalNotFoundException(String terminalKey) {
        super(CODE, "Reference data not found for " + terminalKey);
    }
}
