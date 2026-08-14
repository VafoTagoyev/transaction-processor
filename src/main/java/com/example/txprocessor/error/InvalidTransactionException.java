package com.example.txprocessor.error;

/** The transaction row itself is not processable (missing card_id / terminal_id / amount). */
public class InvalidTransactionException extends PermanentProcessingException {

    public static final String CODE = "INVALID_TRANSACTION";

    public InvalidTransactionException(String message) {
        super(CODE, message);
    }
}
