package org.example.core.admin;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.BrokerExtensionRequest;
import org.apache.kafka.common.requests.BrokerExtensionResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Sends broker-local extension requests without metadata discovery, failover or request retries. */
public final class BrokerExtensionClient implements AutoCloseable {
    private final Time time = Time.SYSTEM;
    private final Metrics metrics;
    private final NetworkClient client;
    private final Node broker;

    public BrokerExtensionClient(Properties properties) {
        String address = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        validateBroker(address);
        broker = new Node(-1, Utils.getHost(address), Utils.getPort(address));
        AbstractConfig config = new AbstractConfig(AdminClientConfig.configDef(), properties, false);
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        LogContext logContext = new LogContext("[BrokerExtensionClient clientId=" + clientId + "] ");
        metrics = new Metrics(time);
        Selector selector = null;
        try {
            selector = new Selector(config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, "broker-extension", ClientUtils.createChannelBuilder(config, time, logContext),
                    logContext);
            client = new NetworkClient(selector, new ManualMetadataUpdater(List.of(broker)), clientId,
                    1, config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                    config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                    config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time, true, new ApiVersions(), logContext, MetadataRecoveryStrategy.NONE);
        } catch (RuntimeException | Error failure) {
            if (selector != null) selector.close();
            metrics.close();
            throw failure;
        }
    }

    /** Validates a single host:port (including bracketed IPv6), without connecting or resolving DNS. */
    public static void validateBroker(String address) {
        if (address == null || address.isBlank() || address.contains(",")
                || address.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("--broker must name exactly one broker as host:port");
        }
        String host = Utils.getHost(address);
        Integer port;
        try {
            port = Utils.getPort(address);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid broker port", e);
        }
        if (host == null || host.isBlank() || port == null || port < 1 || port > 65535
                || !address.equals(Utils.formatAddress(host, port))) {
            throw new IllegalArgumentException("--broker must name exactly one broker as host:port (port 1..65535)");
        }
    }

    /** The timeout includes connection, authentication, API negotiation and response waiting. */
    public BrokerExtensionResponse send(BrokerExtensionRequestData data, int timeoutMs)
            throws InterruptedException {
        if (timeoutMs <= 0) throw new IllegalArgumentException("timeoutMs must be positive");
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        checkInterrupted();
        while (!client.ready(broker, time.milliseconds())) {
            poll(deadline);
            NetworkClientUtils.maybeThrowAuthFailure(client, broker);
            if (client.connectionFailed(broker)) {
                throw new DisconnectException("Connection to broker " + broker.host() + ":" + broker.port() + " failed");
            }
        }
        ClientRequest request = client.newClientRequest(broker.idString(),
                new BrokerExtensionRequest.Builder(data), time.milliseconds(), true, remainingMs(deadline), null);
        client.send(request, time.milliseconds());
        while (true) {
            for (ClientResponse response : poll(deadline)) {
                if (response.requestHeader().correlationId() != request.correlationId()) continue;
                if (response.authenticationException() != null) throw response.authenticationException();
                if (response.versionMismatch() != null) throw response.versionMismatch();
                if (response.wasTimedOut()) throw new TimeoutException("Broker extension response timed out");
                if (response.wasDisconnected()) {
                    throw new DisconnectException("Broker disconnected before the extension response was received");
                }
                return (BrokerExtensionResponse) response.responseBody();
            }
        }
    }

    private List<ClientResponse> poll(long deadline) throws InterruptedException {
        checkInterrupted();
        return client.poll(Math.min(100, remainingMs(deadline)), time.milliseconds());
    }

    private static int remainingMs(long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new TimeoutException("Broker extension request timed out");
        return (int) Math.min(Integer.MAX_VALUE, 1 + (remaining - 1) / 1_000_000L);
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Broker extension request interrupted");
    }

    @Override
    public void close() {
        try {
            client.close();
        } finally {
            metrics.close();
        }
    }
}
