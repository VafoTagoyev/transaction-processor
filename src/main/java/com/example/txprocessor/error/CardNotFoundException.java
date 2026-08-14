package com.example.txprocessor.error;

public class CardNotFoundException extends PermanentProcessingException {

    public static final String CODE = "CARD_NOT_FOUND";

    public CardNotFoundException(String cardKey) {
        super(CODE, "Reference data not found for " + cardKey);
    }
}
