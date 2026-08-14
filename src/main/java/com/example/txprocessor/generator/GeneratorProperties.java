package com.example.txprocessor.generator;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Everything the data generator produces is configurable; nothing is hard coded. */
@ConfigurationProperties(prefix = "generator")
public class GeneratorProperties {

    private boolean enabled = false;

    /** Stop the JVM once generation finishes (how the docker-compose generator service behaves). */
    private boolean exitAfterRun = true;

    /** Wipe transactions, results, statistics and outbox before generating. */
    private boolean truncateFirst = true;

    private long transactions = 1_000_000L;
    private int cards = 100_000;
    private int terminals = 10_000;

    /** Share of transactions whose card and terminal share a bank code. */
    private double internalRatio = 0.80;

    /** Share of transactions pointing at a card id that is deliberately absent from the cache. */
    private double cardMissRatio = 0.01;

    /** Share of transactions pointing at a terminal id that is deliberately absent from the cache. */
    private double terminalMissRatio = 0.01;

    /** Share of amounts placed exactly on the 1 000 000 commission boundary. */
    private double boundaryAmountRatio = 0.02;

    /** Rows per COPY chunk and keys per Redis pipeline flush. */
    private int batchSize = 50_000;

    /** Optional expiry for reference data; zero (default) means no TTL. */
    private Duration referenceDataTtl = Duration.ZERO;

    /** Fixed seed keeps generated datasets reproducible across runs and across machines. */
    private long seed = 20240101L;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isExitAfterRun() { return exitAfterRun; }
    public void setExitAfterRun(boolean exitAfterRun) { this.exitAfterRun = exitAfterRun; }
    public boolean isTruncateFirst() { return truncateFirst; }
    public void setTruncateFirst(boolean truncateFirst) { this.truncateFirst = truncateFirst; }
    public long getTransactions() { return transactions; }
    public void setTransactions(long transactions) { this.transactions = transactions; }
    public int getCards() { return cards; }
    public void setCards(int cards) { this.cards = cards; }
    public int getTerminals() { return terminals; }
    public void setTerminals(int terminals) { this.terminals = terminals; }
    public double getInternalRatio() { return internalRatio; }
    public void setInternalRatio(double internalRatio) { this.internalRatio = internalRatio; }
    public double getCardMissRatio() { return cardMissRatio; }
    public void setCardMissRatio(double cardMissRatio) { this.cardMissRatio = cardMissRatio; }
    public double getTerminalMissRatio() { return terminalMissRatio; }
    public void setTerminalMissRatio(double terminalMissRatio) { this.terminalMissRatio = terminalMissRatio; }
    public double getBoundaryAmountRatio() { return boundaryAmountRatio; }
    public void setBoundaryAmountRatio(double boundaryAmountRatio) { this.boundaryAmountRatio = boundaryAmountRatio; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public Duration getReferenceDataTtl() { return referenceDataTtl; }
    public void setReferenceDataTtl(Duration referenceDataTtl) { this.referenceDataTtl = referenceDataTtl; }
    public long getSeed() { return seed; }
    public void setSeed(long seed) { this.seed = seed; }
}
