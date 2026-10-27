package org.example.apps;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.message.BrokerExtensionResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BrokerExtensionResponse;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class BrokerExtensionTest {
    @TempDir
    Path temporary;

    private final StringWriter stdout = new StringWriter();
    private final StringWriter stderr = new StringWriter();
    private Properties capturedConfig;
    private BrokerExtensionRequestData capturedRequest;
    private int capturedTimeout;
    private int sends;
    private Errors error = Errors.NONE;
    private Exception failure;
    private boolean interruptWhileSending;

    private int execute(String... args) {
        BrokerExtension app = new BrokerExtension((config, request, timeout) -> {
            sends++;
            capturedConfig = config;
            capturedRequest = request;
            capturedTimeout = timeout;
            if (interruptWhileSending) throw new InterruptException("interrupted while polling");
            if (failure != null) throw failure;
            return new BrokerExtensionResponse(new BrokerExtensionResponseData()
                    .setErrorCode(error.code()).setErrorMessage(error.message()));
        });
        CommandLine command = new CommandLine(app);
        command.setOut(new PrintWriter(stdout, true));
        command.setErr(new PrintWriter(stderr, true));
        return command.execute(args);
    }

    @Test
    void sendsRolloutToTheSpecifiedBrokerAndReturnsSuccess() {
        assertEquals(0, execute("--broker", "broker-a:9092", "--target", "monitor-log-rollout",
                "--operation", "flush-and-rollout"));
        assertEquals(1, sends);
        assertEquals("broker-a:9092", capturedConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("monitor-log-rollout", capturedRequest.target());
        assertEquals("flush-and-rollout", capturedRequest.operation());
        assertEquals(30000, capturedTimeout);
        assertEquals(30000, capturedRequest.timeoutMs());
        assertEquals(0, capturedRequest.payloadVersion());
        assertNotEquals(Uuid.ZERO_UUID, capturedRequest.requestId());
        assertTrue(capturedRequest.payload() == null || !capturedRequest.payload().hasRemaining());
        assertTrue(stdout.toString().contains("BROKER_EXTENSION_COMPLETE status=SUCCESS"));
        assertEquals("", stderr.toString());
    }

    @Test
    void supportsExplicitRequestIdAndUtf8Payload() {
        Uuid requestId = Uuid.randomUuid();
        assertEquals(0, execute("--bootstrap-server", "[::1]:9092", "--operation", "divider-and-flush",
                "--request-id", requestId.toString(), "--payload-version", "1", "--payload", "실험 1\n종료",
                "--timeout-ms", "1500"));
        assertEquals("monitor-log", capturedRequest.target());
        assertEquals(requestId, capturedRequest.requestId());
        assertEquals(1, capturedRequest.payloadVersion());
        assertArrayEquals("실험 1\n종료".getBytes(StandardCharsets.UTF_8), Utils.toArray(capturedRequest.payload()));
        assertEquals(1500, capturedRequest.timeoutMs());
        assertEquals(1500, capturedTimeout);
    }

    @Test
    void helpDoesNotSendARequest() {
        assertEquals(0, execute("--help"));
        assertEquals(0, sends);
        assertTrue(stdout.toString().contains("exactly one broker"));
    }

    @Test
    void rejectsInvalidArgumentsBeforeSending() {
        for (String[] args : List.of(
                new String[]{},
                new String[]{"-b", "broker:9092"},
                new String[]{"-b", "one:9092,two:9092", "--operation", "flush"},
                new String[]{"-b", " ", "--operation", "flush"},
                new String[]{"-b", "broker", "--operation", "flush"},
                new String[]{"-b", "broker:0", "--operation", "flush"},
                new String[]{"-b", "broker:65536", "--operation", "flush"},
                new String[]{"-b", "PLAINTEXT://broker:9092", "--operation", "flush"},
                new String[]{"-b", "broker:9092", "--operation", " "},
                new String[]{"-b", "broker:9092", "--operation", "flush", "--target", "bad target"},
                new String[]{"-b", "broker:9092", "--operation", "flush", "--timeout-ms", "0"},
                new String[]{"-b", "broker:9092", "--operation", "flush", "--timeout-ms", "-1"},
                new String[]{"-b", "broker:9092", "--operation", "flush", "--payload-version", "-1"},
                new String[]{"-b", "broker:9092", "--operation", "flush", "--request-id", "invalid!"},
                new String[]{"-b", "broker:9092", "--operation", "flush", "--result-file", "result.json"})) {
            assertEquals(2, execute(args), String.join(" ", args));
        }
        assertEquals(0, sends);
    }

    @Test
    void commandConfigSupportsSecurityAndCannotRedirectTheExplicitBroker() throws Exception {
        Path config = temporary.resolve("client.properties");
        Files.writeString(config, "bootstrap.servers=other:9092,another:9092\nclient.id=my-control\n"
                + "security.protocol=SASL_SSL\nrequest.timeout.ms=999999\n");
        assertEquals(0, execute("-b", "selected:9092", "--operation", "flush", "--timeout-ms", "1500",
                "--command-config", config.toString()));
        assertEquals("selected:9092", capturedConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("my-control", capturedConfig.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));
        assertEquals("SASL_SSL", capturedConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("1500", capturedConfig.getProperty(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    void missingCommandConfigFailsWithoutSending() {
        assertEquals(1, execute("-b", "broker:9092", "--operation", "flush", "--command-config",
                temporary.resolve("missing.properties").toString()));
        assertEquals(0, sends);
    }

    @Test
    void brokerErrorsReturnFailureWithoutRetrying() {
        for (Errors brokerError : List.of(Errors.CLUSTER_AUTHORIZATION_FAILED, Errors.UNSUPPORTED_VERSION,
                Errors.INVALID_REQUEST, Errors.REQUEST_TIMED_OUT)) {
            error = brokerError;
            assertEquals(1, execute("-b", "broker:9092", "--operation", "flush"));
        }
        assertEquals(4, sends);
        assertFalse(stdout.toString().contains("BROKER_EXTENSION_COMPLETE"));
        assertTrue(stderr.toString().contains("BROKER_EXTENSION_FAILED"));
    }

    @Test
    void timeoutAndConnectionFailureReturnFailureWithoutRetrying() {
        failure = new TimeoutException("deadline elapsed");
        assertEquals(1, execute("-b", "broker:9092", "--operation", "flush"));
        failure = new IllegalStateException("connection failed");
        assertEquals(1, execute("-b", "broker:9092", "--operation", "flush"));
        assertEquals(2, sends);
        assertFalse(stdout.toString().contains("BROKER_EXTENSION_COMPLETE"));
    }

    @Test
    void interruptionReturns130AndPreservesInterruptedStatus() {
        try {
            failure = new InterruptedException("interrupted");
            assertEquals(130, execute("-b", "broker:9092", "--operation", "flush"));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        try {
            failure = null;
            interruptWhileSending = true;
            assertEquals(130, execute("-b", "broker:9092", "--operation", "flush"));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals(2, sends);
        assertFalse(stdout.toString().contains("BROKER_EXTENSION_COMPLETE"));
    }
}
