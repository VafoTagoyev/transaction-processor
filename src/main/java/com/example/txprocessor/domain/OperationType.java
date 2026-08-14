package com.example.txprocessor.domain;

public enum OperationType {
    /** Card and terminal belong to the same bank. */
    INTERNAL,
    /** Card and terminal belong to different banks. */
    EXTERNAL
}
