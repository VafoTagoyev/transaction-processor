package com.example.txprocessor.processing;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.logging.LogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A fixed set of platform threads, each running a take-and-process loop.
 *
 * <h2>Why a fixed platform-thread pool and not virtual threads</h2>
 * The per-transaction work is one cache round trip plus one short JDBC transaction. Both are
 * blocking, and the JDBC part needs a HikariCP connection. The real ceiling on useful
 * concurrency is therefore the connection pool, not the thread count: running 10 000 virtual
 * threads against a 20-connection pool converts thread waiting into connection-pool waiting and
 * adds nothing but queueing latency and 10 000 sets of in-flight objects on the heap. On top of
 * that, on Java 21 a virtual thread that blocks inside a {@code synchronized} block pins its
 * carrier thread, and JDBC drivers still use {@code synchronized} liberally — so the scalability
 * argument for virtual threads is weakest exactly where this workload spends its time.
 *
 * <p>A fixed pool sized in the same range as the connection pool gives predictable memory,
 * predictable database load, and a queue depth we can actually observe. If this service ever
 * became dominated by long, non-blocking, non-JDBC I/O, virtual threads would be the right
 * answer — and the change would still require an explicit semaphore capped at the HikariCP pool
 * size in front of the persistence step, otherwise the pool becomes the new failure point.
 *
 * <h2>Starvation and pool exhaustion</h2>
 * Workers never wait on each other and never hold a connection while doing I/O, so no worker
 * can block another. A worker holds a connection only for the duration of the persistence
 * transaction; with {@code workers <= maximum-pool-size} no worker ever waits for a connection,
 * and the poller uses one further connection for its claim statement.
 */
@Component
public class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    /** Poll timeout of the worker loop: how quickly a worker notices a stop request. */
    private static final long TAKE_TIMEOUT_MILLIS = 200;

    private final ProcessingQueue queue;
    private final TransactionProcessingService processingService;
    private final ClaimReleaser claimReleaser;
    private final int workerCount;
    private final String instanceId;

    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;
    private volatile boolean draining;
    private CountDownLatch stopped;

    public WorkerPool(ProcessingQueue queue,
                      TransactionProcessingService processingService,
                      ClaimReleaser claimReleaser,
                      ProcessorProperties properties) {
        this.queue = queue;
        this.processingService = processingService;
        this.claimReleaser = claimReleaser;
        this.workerCount = properties.getWorkers();
        this.instanceId = properties.getInstanceId();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        draining = false;
        stopped = new CountDownLatch(workerCount);
        for (int i = 0; i < workerCount; i++) {
            Thread thread = new Thread(this::workerLoop, "tx-worker-" + i);
            thread.setDaemon(false);
            thread.setUncaughtExceptionHandler((t, e) -> log.error("Worker {} died unexpectedly", t.getName(), e));
            threads.add(thread);
            thread.start();
        }
        log.info("Started {} workers on instance {}", workerCount, instanceId);
    }

    /**
     * Graceful stop: workers keep draining the queue until it is empty or the grace period
     * expires, then everything still queued is handed back to the pool as NEW so another
     * instance can pick it up immediately instead of waiting for the lease to expire.
     */
    public synchronized void stop(Duration grace) {
        if (!running) {
            return;
        }
        draining = true;
        try {
            if (!stopped.await(grace.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Workers did not finish within the {}s grace period; interrupting", grace.toSeconds());
                threads.forEach(Thread::interrupt);
                stopped.await(Math.max(1, grace.toMillis() / 5), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            threads.clear();
            List<ClaimedTransaction> abandoned = queue.drain();
            claimReleaser.releaseAll(abandoned);
            log.info("Worker pool stopped; released {} unstarted claims", abandoned.size());
        }
    }

    private void workerLoop() {
        LogContext.putInstanceId(instanceId);
        try {
            while (true) {
                if (draining && queue.size() == 0) {
                    return;
                }
                ClaimedTransaction transaction;
                try {
                    transaction = queue.poll(TAKE_TIMEOUT_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (transaction == null) {
                    continue;
                }
                // process() is total: it converts every failure into a durable status transition
                // and never propagates, so a single poison transaction cannot kill a worker.
                processingService.process(transaction);
            }
        } finally {
            stopped.countDown();
        }
    }

    public boolean isRunning() {
        return running;
    }
}
