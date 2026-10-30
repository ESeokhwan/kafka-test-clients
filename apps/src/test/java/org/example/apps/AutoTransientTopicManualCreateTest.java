package org.example.apps;

import org.apache.kafka.clients.producer.TransientTopicHandle;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AutoTransientTopicManualCreateTest {
    private AutoTransientTopicProducerMetrics app(String... options) {
        AutoTransientTopicProducerMetrics app = new AutoTransientTopicProducerMetrics();
        List<String> args = new ArrayList<>(List.of("-b", "mock:9092", "-p", "manual"));
        args.addAll(List.of(options));
        new CommandLine(app).parseArgs(args.toArray(String[]::new));
        return app;
    }

    private TransientTopicHandle handle(String name) {
        return new TransientTopicHandle(Uuid.randomUuid(), name);
    }

    @Test
    void defaultModeDoesNotExplicitlyCreateTopics() throws Exception {
        assertNull(app().prepareTopicForWorker("manual_0_0", null, name -> {
            fail("Default mode must continue to use name-based auto creation");
            return null;
        }, 1000));
    }

    @Test
    void waitsForTheCreationAcknowledgementBeforeMakingTheWorkerReadyToProduce() throws Exception {
        AutoTransientTopicProducerMetrics app = app("--need-manual-create");
        TransientTopicHandle handle = handle("manual_0_7");
        CompletableFuture<TransientTopicHandle> creation = new CompletableFuture<>();
        CountDownLatch requested = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<AutoTransientTopicProducerMetrics.PreparedTopic> ready = executor.submit(() ->
                    app.prepareTopicForWorker(handle.name(), null, name -> {
                        assertEquals(handle.name(), name);
                        requests.incrementAndGet();
                        requested.countDown();
                        return creation;
                    }, 2000));
            assertTrue(requested.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> ready.get(50, TimeUnit.MILLISECONDS));
            creation.complete(handle);
            AutoTransientTopicProducerMetrics.PreparedTopic topic = ready.get(1, TimeUnit.SECONDS);
            assertSame(handle, topic.handle());
            assertTrue(topic.requestMs() > 0);
            assertTrue(topic.latencyMs() >= 0);
            assertEquals(1, requests.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void precreatedTopicsAreReusedWithoutAnotherCreateRequest() throws Exception {
        AutoTransientTopicProducerMetrics app = app("--precreate", "--need-manual-create");
        AutoTransientTopicProducerMetrics.PreparedTopic prepared =
                new AutoTransientTopicProducerMetrics.PreparedTopic(handle("manual_0_0"), 100, 25);
        assertSame(prepared, app.prepareTopicForWorker(prepared.handle().name(), prepared, name -> {
            fail("--precreate already created the topic");
            return null;
        }, 1000));
    }

    @Test
    void creationFailureIsPropagatedWithoutRetryingOrFallingBackToAutoCreation() {
        IllegalStateException rejected = new IllegalStateException("manual create rejected");
        AtomicInteger requests = new AtomicInteger();
        ExecutionException failure = assertThrows(ExecutionException.class, () ->
                app("--need-manual-create").prepareTopicForWorker("manual_0_0", null, name -> {
                    requests.incrementAndGet();
                    return CompletableFuture.failedFuture(rejected);
                }, 1000));
        assertSame(rejected, failure.getCause());
        assertEquals(1, requests.get());
    }

    @Test
    void pendingCreationTimesOutInsteadOfAllowingProduction() {
        AtomicInteger requests = new AtomicInteger();
        assertThrows(TimeoutException.class, () -> app("--need-manual-create").prepareTopicForWorker(
                "manual_0_0", null, name -> {
                    requests.incrementAndGet();
                    return new CompletableFuture<>();
                }, 50));
        assertEquals(1, requests.get());
    }

    @Test
    void failedWorkerDoesNotPreventLaterWorkersFromCreating() throws Exception {
        AutoTransientTopicProducerMetrics app = app("--need-manual-create");
        ExecutionException failure = assertThrows(ExecutionException.class, () ->
                app.prepareTopicForWorker("manual_0_0", null,
                        name -> CompletableFuture.failedFuture(new IllegalStateException("creation rejected")), 1000));
        app.failManualCreation("manual_0_0", failure);

        TransientTopicHandle next = handle("manual_0_1");
        assertSame(next, app.prepareTopicForWorker(next.name(), null,
                name -> CompletableFuture.completedFuture(next), 1000).handle());
    }

    @Test
    void timedOutWorkerDoesNotStopPendingOrNewWorkers() throws Exception {
        AutoTransientTopicProducerMetrics app = app("--need-manual-create");
        TransientTopicHandle pending = handle("manual_0_0");
        CompletableFuture<TransientTopicHandle> completion = new CompletableFuture<>();
        CountDownLatch requested = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<AutoTransientTopicProducerMetrics.PreparedTopic> existing = executor.submit(() ->
                    app.prepareTopicForWorker(pending.name(), null, name -> {
                        requested.countDown();
                        return completion;
                    }, 5000));
            assertTrue(requested.await(1, TimeUnit.SECONDS));

            TimeoutException failure = assertThrows(TimeoutException.class, () ->
                    app.prepareTopicForWorker("manual_0_1", null, name -> new CompletableFuture<>(), 50));
            app.failManualCreation("manual_0_1", failure);
            assertFalse(existing.isDone());

            TransientTopicHandle next = handle("manual_0_2");
            assertSame(next, app.prepareTopicForWorker(next.name(), null,
                    name -> CompletableFuture.completedFuture(next), 1000).handle());
            completion.complete(pending);
            assertSame(pending, existing.get(1, TimeUnit.SECONDS).handle());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void interruptionPreventsCreation() {
        AutoTransientTopicProducerMetrics app = app("--need-manual-create");
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> app.prepareTopicForWorker("manual_0_0", null, name -> {
                fail("An interrupted worker must not create a topic");
                return null;
            }, 1000));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void helpDescribesTheOptionWithoutConnectingToKafka() {
        StringWriter output = new StringWriter();
        CommandLine command = new CommandLine(new AutoTransientTopicProducerMetrics());
        command.setOut(new PrintWriter(output, true));
        assertEquals(0, command.execute("--help"));
        assertTrue(output.toString().contains("--need-manual-create"));
    }
}
