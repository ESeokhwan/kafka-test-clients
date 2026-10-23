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

import static org.junit.jupiter.api.Assertions.*;

class AutoTransientTopicPrecreateTest {
    private AutoTransientTopicProducerMetrics app(String... extraArgs) {
        AutoTransientTopicProducerMetrics app = new AutoTransientTopicProducerMetrics();
        List<String> args = new ArrayList<>(List.of("-b", "mock:9092", "-p", "prepared", "--precreate"));
        args.addAll(List.of(extraArgs));
        new CommandLine(app).parseArgs(args.toArray(String[]::new));
        return app;
    }

    private TransientTopicHandle handle(String name) {
        return new TransientTopicHandle(Uuid.randomUuid(), name);
    }

    private long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    }

    @Test
    void waitsForEveryCreationAcknowledgementBeforeReturningHandlesForProduction() throws Exception {
        AutoTransientTopicProducerMetrics app = app("--topic-cnt", "2");
        TransientTopicHandle first = handle("prepared_0_0");
        TransientTopicHandle last = handle("prepared_0_1");
        CompletableFuture<TransientTopicHandle> lastCompletion = new CompletableFuture<>();
        CountDownLatch lastRequested = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<AutoTransientTopicProducerMetrics.PreparedTopic>> prepared = executor.submit(() ->
                    app.precreateTopics(name -> {
                        if (name.equals(first.name())) return CompletableFuture.completedFuture(first);
                        assertEquals(last.name(), name);
                        lastRequested.countDown();
                        return lastCompletion;
                    }, deadline()));

            assertTrue(lastRequested.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> prepared.get(50, TimeUnit.MILLISECONDS),
                    "Production must wait for the last create response");
            lastCompletion.complete(last);
            List<AutoTransientTopicProducerMetrics.PreparedTopic> topics = prepared.get(2, TimeUnit.SECONDS);
            assertEquals(List.of(first, last), topics.stream().map(
                    AutoTransientTopicProducerMetrics.PreparedTopic::handle).toList());
            assertTrue(topics.stream().allMatch(topic -> topic.requestMs() > 0 && topic.latencyMs() >= 0));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void usesDerivedTopicCountAndTheSameNamesAsProduction() throws Exception {
        AutoTransientTopicProducerMetrics app = app("--topic-create-hz", "2", "--run-duration-minutes", "0.025",
                "--client-index", "4", "--start-index", "7");
        List<String> names = new ArrayList<>();
        List<AutoTransientTopicProducerMetrics.PreparedTopic> topics = app.precreateTopics(name -> {
            names.add(name);
            return CompletableFuture.completedFuture(handle(name));
        }, deadline());

        assertEquals(List.of("prepared_4_7", "prepared_4_8", "prepared_4_9"), names);
        assertEquals(names, topics.stream().map(topic -> topic.handle().name()).toList());
    }

    @Test
    void failedCreationAbortsPreparationWithoutReturningPartialHandles() {
        AutoTransientTopicProducerMetrics app = app("--topic-cnt", "3");
        List<String> names = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("creation rejected");

        ExecutionException error = assertThrows(ExecutionException.class, () -> app.precreateTopics(name -> {
            names.add(name);
            return names.size() == 1 ? CompletableFuture.completedFuture(handle(name))
                    : CompletableFuture.failedFuture(failure);
        }, deadline()));

        assertSame(failure, error.getCause());
        assertEquals(List.of("prepared_0_0", "prepared_0_1"), names);
    }

    @Test
    void pendingCreationTimesOutWithoutReturningPartialHandles() {
        AutoTransientTopicProducerMetrics app = app("--topic-cnt", "2");
        List<String> names = new ArrayList<>();
        assertThrows(TimeoutException.class, () -> app.precreateTopics(name -> {
            names.add(name);
            return new CompletableFuture<>();
        }, System.nanoTime() + TimeUnit.SECONDS.toNanos(1)));
        assertEquals(List.of("prepared_0_0"), names);
    }

    @Test
    void interruptedPreparationDoesNotCreateAnyTopics() {
        AutoTransientTopicProducerMetrics app = app("--topic-cnt", "1");
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> app.precreateTopics(name -> {
                fail("Interrupted preparation must not create a topic");
                return null;
            }, deadline()));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void invalidPreparationTimeoutFailsBeforeConnectingToKafka() {
        for (String timeout : List.of("0", "-1")) {
            StringWriter errors = new StringWriter();
            CommandLine command = new CommandLine(new AutoTransientTopicProducerMetrics());
            command.setErr(new PrintWriter(errors, true));
            assertEquals(1, command.execute("-b", "mock:9092", "-p", "prepared", "--precreate",
                    "--topic-prepare-timeout-seconds", timeout));
            assertTrue(errors.toString().contains("--topic-prepare-timeout-seconds must be positive"));
        }
    }
}
