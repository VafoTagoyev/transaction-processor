package com.example.txprocessor.processing;

import com.example.txprocessor.domain.CardInfo;
import com.example.txprocessor.domain.EnrichedTransaction;
import com.example.txprocessor.domain.OperationType;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.domain.TerminalInfo;
import com.example.txprocessor.error.InvalidReferenceDataException;
import com.example.txprocessor.error.InvalidTransactionException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Pure business logic: no I/O, no clock, no randomness. That is deliberate — it makes the
 * rules exhaustively unit-testable and means a retry recomputes exactly the same result,
 * which is what allows recovery to safely reprocess a transaction after a crash.
 */
@Component
public class BusinessClassifier {

    private final CommissionCalculator commissionCalculator;

    public BusinessClassifier(CommissionCalculator commissionCalculator) {
        this.commissionCalculator = commissionCalculator;
    }

    public ProcessingResult classify(EnrichedTransaction enriched) {
        CardInfo card = enriched.card();
        TerminalInfo terminal = enriched.terminal();
        BigDecimal amount = enriched.transaction().amount();

        if (amount == null) {
            throw new InvalidTransactionException("Transaction amount is null");
        }
        if (amount.signum() < 0) {
            // The assignment defines commission only for non-negative amounts. Rather than
            // invent a refund rule, a negative amount is rejected as unprocessable input.
            throw new InvalidTransactionException("Transaction amount is negative: " + amount);
        }
        if (isBlank(card.bankCode()) || isBlank(terminal.bankCode())) {
            throw new InvalidReferenceDataException("bankCode is missing on card or terminal reference data");
        }

        OperationType operationType = card.bankCode().equals(terminal.bankCode())
                ? OperationType.INTERNAL
                : OperationType.EXTERNAL;

        BigDecimal commission = commissionCalculator.calculate(operationType, amount);

        return new ProcessingResult(
                enriched.transaction().id(),
                enriched.transaction().externalId(),
                card.clientId(),
                card.account(),
                card.productId(),
                terminal.merchantId(),
                terminal.branchCode(),
                amount,
                commission,
                operationType);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
