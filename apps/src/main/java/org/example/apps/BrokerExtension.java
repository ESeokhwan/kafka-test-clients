package org.example.apps;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BrokerExtensionResponse;
import org.example.core.admin.BrokerExtensionClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** Broker-local extension CLI packaged with the experiment clients. */
@Command(name = "broker-extension", mixinStandardHelpOptions = true,
        description = "Send one extension request to exactly one broker, without automatic retries.")
public final class BrokerExtension implements Callable<Integer> {
    @Option(names = {"-b", "--broker", "--bootstrap-server"}, required = true,
            description = "Target broker as host:port (exactly one; bracket IPv6 addresses)")
    private String broker;

    @Option(names = "--target", defaultValue = "monitor-log", description = "Registered extension target")
    private String target;

    @Option(names = "--operation", required = true, description = "Target-specific operation, e.g. flush-and-rollout")
    private String operation;

    @Option(names = "--payload", description = "Optional UTF-8 request payload")
    private String payload;

    @Option(names = "--payload-version", defaultValue = "0", description = "Payload schema version")
    private short payloadVersion;

    @Option(names = "--request-id", description = "Kafka UUID (URL-safe Base64); generated when omitted")
    private String requestId;

    @Option(names = "--timeout-ms", defaultValue = "30000",
            description = "Whole request timeout in milliseconds (default: ${DEFAULT-VALUE})")
    private int timeoutMs;

    @Option(names = "--command-config", description = "Kafka client/security properties file")
    private Path commandConfig;

    @Spec
    private CommandSpec spec;

    @FunctionalInterface
    interface RequestSender {
        BrokerExtensionResponse send(Properties config, BrokerExtensionRequestData request, int timeoutMs)
                throws Exception;
    }

    private final RequestSender sender;

    public BrokerExtension() {
        this((config, request, remainingMs) -> {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(remainingMs);
            try (BrokerExtensionClient client = new BrokerExtensionClient(config)) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new TimeoutException("Broker extension client setup timed out");
                return client.send(request, (int) (1 + (remaining - 1) / 1_000_000L));
            }
        });
    }

    BrokerExtension(RequestSender sender) {
        this.sender = sender;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new BrokerExtension()).execute(args));
    }

    @Override
    public Integer call() {
        BrokerExtensionRequestData request = createRequest();
        try {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Request interrupted");
            Properties config = new Properties();
            if (commandConfig != null) {
                try (InputStream input = Files.newInputStream(commandConfig)) {
                    config.load(input);
                }
            }
            config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
            config.putIfAbsent(CommonClientConfigs.CLIENT_ID_CONFIG, "experiment-broker-extension");
            config.setProperty(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(timeoutMs));
            spec.commandLine().getOut().printf(
                    "BROKER_EXTENSION_STARTED broker=%s requestId=%s target=%s operation=%s%n",
                    broker, request.requestId(), target, operation);
            spec.commandLine().getOut().flush();
            BrokerExtensionResponse response = sender.send(config, request, timeoutMs);
            if (response.error().error() != Errors.NONE) {
                throw response.error().exception();
            }
            spec.commandLine().getOut().println("BROKER_EXTENSION_COMPLETE status=SUCCESS");
            spec.commandLine().getOut().flush();
            return 0;
        } catch (InterruptedException | InterruptException e) {
            Thread.currentThread().interrupt();
            reportFailure(e);
            return 130;
        } catch (Exception e) {
            reportFailure(e);
            return 1;
        }
    }

    private BrokerExtensionRequestData createRequest() {
        try {
            BrokerExtensionClient.validateBroker(broker);
            if (!target.matches("[a-z0-9][a-z0-9._-]{0,63}")) {
                throw new IllegalArgumentException("--target must match [a-z0-9][a-z0-9._-]{0,63}");
            }
            if (operation.isBlank() || timeoutMs <= 0 || payloadVersion < 0) {
                throw new IllegalArgumentException("--operation must be non-empty, --timeout-ms positive, "
                        + "and --payload-version non-negative");
            }
            BrokerExtensionRequestData request = new BrokerExtensionRequestData()
                    .setRequestId(requestId == null ? Uuid.randomUuid() : Uuid.fromString(requestId))
                    .setTarget(target).setOperation(operation).setPayloadVersion(payloadVersion).setTimeoutMs(timeoutMs);
            if (payload != null) request.setPayload(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)));
            return request;
        } catch (IllegalArgumentException e) {
            throw new CommandLine.ParameterException(spec.commandLine(), e.getMessage(), e);
        }
    }

    private void reportFailure(Exception failure) {
        String detail = String.valueOf(failure.getMessage()).replace('\n', ' ').replace('\r', ' ');
        spec.commandLine().getErr().printf("BROKER_EXTENSION_FAILED type=%s detail=%s%n",
                failure.getClass().getSimpleName(), detail);
        spec.commandLine().getErr().flush();
    }
}
