package org.example.apps;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ResetTransientTopicsOptions;
import org.apache.kafka.clients.admin.ResetTransientTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ResetTransientTopicsTest {
    @TempDir
    Path temporary;

    private final KafkaFutureImpl<Void> completion = new KafkaFutureImpl<>();
    private final StringWriter stdout = new StringWriter();
    private final StringWriter stderr = new StringWriter();
    private final List<String> events = new ArrayList<>();
    private Properties capturedConfig;
    private ResetTransientTopicsOptions capturedOptions;
    private Duration closeTimeout;
    private int creations;
    private boolean failCreation;
    private boolean failClose;

    private int execute(String... args) {
        ResetTransientTopics app = new ResetTransientTopics(config -> {
            creations++;
            if (failCreation) throw new IllegalStateException("invalid client configuration");
            capturedConfig = new Properties();
            capturedConfig.putAll(config);
            return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[]{Admin.class},
                    (proxy, method, values) -> {
                        switch (method.getName()) {
                            case "resetTransientTopics":
                                events.add("reset");
                                capturedOptions = (ResetTransientTopicsOptions) values[0];
                                // The Kafka result constructor is package-private; no real client is created.
                                Constructor<ResetTransientTopicsResult> constructor =
                                        ResetTransientTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);
                                constructor.setAccessible(true);
                                return constructor.newInstance(completion);
                            case "close":
                                events.add("close");
                                closeTimeout = (Duration) values[0];
                                if (failClose) throw new IllegalStateException("close failed");
                                return null;
                            default:
                                throw new AssertionError("Unexpected Admin method: " + method);
                        }
                    });
        });
        CommandLine command = new CommandLine(app);
        command.setOut(new PrintWriter(stdout, true));
        command.setErr(new PrintWriter(stderr, true));
        return command.execute(args);
    }

    @Test
    void waitsForResetAndClosesBeforeReportingSuccess() {
        completion.complete(null);
        assertEquals(0, execute("--bootstrap-server", "broker-a:9092,broker-b:9092",
                "--timeout-ms", "1500"));
        assertEquals(List.of("reset", "close"), events);
        assertEquals(1500, capturedOptions.timeoutMs());
        assertEquals("broker-a:9092,broker-b:9092",
                capturedConfig.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("transient-topic-reset", capturedConfig.getProperty(AdminClientConfig.CLIENT_ID_CONFIG));
        assertFalse(closeTimeout.isNegative());
        assertTrue(closeTimeout.compareTo(Duration.ofMillis(1500)) <= 0);
        assertTrue(stdout.toString().contains("RESET_COMPLETE status=SUCCESS"));
        assertEquals("", stderr.toString());
    }

    @Test
    void defaultTimeoutMatchesExperimentCleanupAndBrokerAliasWorks() {
        completion.complete(null);
        assertEquals(0, execute("-b", "mock:9092"));
        assertEquals(300000, capturedOptions.timeoutMs());
    }

    @Test
    void helpDoesNotCreateAnAdminClient() {
        assertEquals(0, execute("--help"));
        assertEquals(0, creations);
        assertTrue(stdout.toString().contains("ALL transient"));
    }

    @Test
    void invalidArgumentsAreRejectedBeforeCreatingAClient() {
        for (String[] args : List.of(
                new String[]{},
                new String[]{"-b", " "},
                new String[]{"-b", "mock:9092", "--timeout-ms", "0"},
                new String[]{"-b", "mock:9092", "--timeout-ms", "-1"},
                new String[]{"-b", "mock:9092", "--timeout-ms", "invalid"})) {
            assertEquals(2, execute(args));
        }
        assertEquals(0, creations);
    }

    @Test
    void failedResetReturnsFailureAndDoesNotReportCompletion() {
        completion.completeExceptionally(new IllegalStateException("reset disabled"));
        assertEquals(1, execute("--brokers", "mock:9092"));
        assertEquals(List.of("reset", "close"), events);
        assertTrue(stderr.toString().contains("RESET_FAILED type=IllegalStateException detail=reset disabled"));
        assertFalse(stdout.toString().contains("RESET_COMPLETE"));
    }

    @Test
    void pendingResetTimesOutWithoutRetryAndClosesClient() {
        assertEquals(1, execute("-b", "mock:9092", "--timeout-ms", "20"));
        assertEquals(List.of("reset", "close"), events);
        assertTrue(stderr.toString().contains("TimeoutException"));
        assertFalse(stdout.toString().contains("RESET_COMPLETE"));
        assertEquals(Duration.ZERO, closeTimeout);
    }

    @Test
    void interruptionClosesClientAndPreservesInterruptedStatus() {
        try {
            Thread.currentThread().interrupt();
            assertEquals(130, execute("-b", "mock:9092"));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(List.of("reset", "close"), events);
            assertFalse(stdout.toString().contains("RESET_COMPLETE"));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void commandConfigIsLoadedAndExplicitBrokersTakePrecedence() throws Exception {
        Path config = temporary.resolve("admin.properties");
        Files.writeString(config, "bootstrap.servers=ignored:9092\nclient.id=custom-reset\n"
                + "security.protocol=SASL_PLAINTEXT\n");
        completion.complete(null);
        assertEquals(0, execute("-b", "selected:9092", "--command-config", config.toString()));
        assertEquals("selected:9092", capturedConfig.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("custom-reset", capturedConfig.getProperty(AdminClientConfig.CLIENT_ID_CONFIG));
        assertEquals("SASL_PLAINTEXT", capturedConfig.getProperty("security.protocol"));
    }

    @Test
    void missingCommandConfigFailsBeforeCreatingClient() {
        assertEquals(1, execute("-b", "mock:9092", "--command-config",
                temporary.resolve("missing.properties").toString()));
        assertEquals(0, creations);
        assertFalse(stdout.toString().contains("RESET_COMPLETE"));
    }

    @Test
    void clientCreationFailureReturnsFailure() {
        failCreation = true;
        assertEquals(1, execute("-b", "mock:9092"));
        assertEquals(1, creations);
        assertTrue(events.isEmpty());
        assertTrue(stderr.toString().contains("invalid client configuration"));
    }

    @Test
    void closeFailureIsNotReportedAsSuccess() {
        completion.complete(null);
        failClose = true;
        assertEquals(1, execute("-b", "mock:9092"));
        assertFalse(stdout.toString().contains("RESET_COMPLETE"));
        assertTrue(stderr.toString().contains("close failed"));
    }
}
