package com.example.txprocessor.processing;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.metrics.ProcessorMetrics;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The bounded hand-off between the poller and the workers, and the single place where this
 * service's memory footprint is decided.
 *
 * <p>{@link ArrayBlockingQueue} is chosen over a linked queue precisely because its capacity is
 * fixed at construction and its backing array is allocated up front: the queue cannot grow, so
 * "the reader is faster than the writer" (the 50 000/s vs 10 000/s scenario in the assignment)
 * degrades into the poller idling rather than into an OutOfMemoryError. Worst case heap held by
 * the pipeline is {@code queue-capacity + workers} claimed transactions, which is a
 * configuration decision rather than a function of how far behind the service is.
 */
@Component
public class ProcessingQueue {

    private final BlockingQueue<ClaimedTransaction> queue;
    private final int capacity;

    public ProcessingQueue(ProcessorProperties properties, ProcessorMetrics metrics) {
        this.capacity = properties.getQueueCapacity();
        this.queue = new ArrayBlockingQueue<>(capacity);
        metrics.bindQueue(queue::size, capacity);
    }

    public boolean offer(ClaimedTransaction transaction, long timeoutMillis) throws InterruptedException {
        return queue.offer(transaction, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public ClaimedTransaction poll(long timeoutMillis) throws InterruptedException {
        return queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }

    public List<ClaimedTransaction> drain() {
        List<ClaimedTransaction> drained = new ArrayList<>();
        queue.drainTo(drained);
        return drained;
    }
}
