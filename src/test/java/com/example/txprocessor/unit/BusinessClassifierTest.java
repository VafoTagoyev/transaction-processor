package com.example.txprocessor.unit;

import com.example.txprocessor.domain.CardInfo;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.EnrichedTransaction;
import com.example.txprocessor.domain.OperationType;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.domain.TerminalInfo;
import com.example.txprocessor.error.InvalidReferenceDataException;
import com.example.txprocessor.error.InvalidTransactionException;
import com.example.txprocessor.processing.BusinessClassifier;
import com.example.txprocessor.processing.CommissionCalculator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BusinessClassifierTest {

    private final BusinessClassifier classifier = new BusinessClassifier(new CommissionCalculator());

    @Test
    @DisplayName("Same bank code on card and terminal makes the operation INTERNAL and free")
    void sameBankIsInternal() {
        ProcessingResult result = classifier.classify(enriched("00444", "00444", "5000000"));

        assertThat(result.operationType()).isEqualTo(OperationType.INTERNAL);
        assertThat(result.commission()).isEqualByComparingTo("0.00");
    }

    @Test
    @DisplayName("Different bank codes make the operation EXTERNAL and chargeable")
    void differentBankIsExternal() {
        ProcessingResult result = classifier.classify(enriched("00444", "00445", "1000"));

        assertThat(result.operationType()).isEqualTo(OperationType.EXTERNAL);
        assertThat(result.commission()).isEqualByComparingTo("10.00");
    }

    @Test
    @DisplayName("Enrichment fields are copied onto the result verbatim")
    void resultCarriesEnrichmentFields() {
        ProcessingResult result = classifier.classify(enriched("00444", "00445", "1000"));

        assertThat(result.clientId()).isEqualTo("12345");
        assertThat(result.account()).isEqualTo("20208000123456789001");
        assertThat(result.productId()).isEqualTo("VISA_GOLD");
        assertThat(result.merchantId()).isEqualTo("M1001");
        assertThat(result.branchCode()).isEqualTo("001");
        assertThat(result.externalId()).isEqualTo("TX-1");
    }

    @Test
    @DisplayName("A missing bank code is unusable reference data, not an EXTERNAL operation")
    void missingBankCodeIsRejected() {
        assertThatThrownBy(() -> classifier.classify(enriched(null, "00445", "1000")))
                .isInstanceOf(InvalidReferenceDataException.class);
        assertThatThrownBy(() -> classifier.classify(enriched("00444", "  ", "1000")))
                .isInstanceOf(InvalidReferenceDataException.class);
    }

    @Test
    @DisplayName("Null and negative amounts are permanent input errors")
    void invalidAmountsAreRejected() {
        assertThatThrownBy(() -> classifier.classify(enriched("00444", "00445", null)))
                .isInstanceOf(InvalidTransactionException.class);
        assertThatThrownBy(() -> classifier.classify(enriched("00444", "00445", "-1")))
                .isInstanceOf(InvalidTransactionException.class);
    }

    @Test
    @DisplayName("Classification is pure: the same input always yields the same result")
    void classificationIsDeterministic() {
        EnrichedTransaction input = enriched("00444", "00445", "1000000.00");

        ProcessingResult first = classifier.classify(input);
        ProcessingResult second = classifier.classify(input);

        // This is what makes it safe for recovery to recompute a transaction after a crash.
        assertThat(first).isEqualTo(second);
    }

    private EnrichedTransaction enriched(String cardBank, String terminalBank, String amount) {
        ClaimedTransaction transaction = new ClaimedTransaction(
                1L, "TX-1", "100001", "50001",
                amount == null ? null : new BigDecimal(amount),
                "UZS", "PURCHASE", 0, UUID.randomUUID());
        CardInfo card = new CardInfo("12345", "20208000123456789001", "VISA_GOLD", cardBank);
        TerminalInfo terminal = new TerminalInfo("M1001", "001", "POS", terminalBank);
        return new EnrichedTransaction(transaction, card, terminal);
    }
}
