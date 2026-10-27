package org.example.apps;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.message.BrokerExtensionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the real Kafka transport against a loopback protocol peer, without a running Kafka cluster. */
class BrokerExtensionNetworkTest {
    private final StringWriter stdout = new StringWriter();
    private final StringWriter stderr = new StringWriter();

    private int execute(int port, String... extra) {
        String[] args = new String[6 + extra.length];
        args[0] = "--broker";
        args[1] = "127.0.0.1:" + port;
        args[2] = "--operation";
        args[3] = "flush-and-rollout";
        args[4] = "--timeout-ms";
        args[5] = "2000";
        System.arraycopy(extra, 0, args, 6, extra.length);
        CommandLine command = new CommandLine(new BrokerExtension());
        command.setOut(new PrintWriter(stdout, true));
        command.setErr(new PrintWriter(stderr, true));
        return command.execute(args);
    }

    @Test
    void negotiatesApiVersionSendsTheRequestAndClosesTheConnection() throws Exception {
        try (Peer peer = new Peer(true, Errors.NONE)) {
            Uuid requestId = Uuid.randomUuid();
            assertEquals(0, execute(peer.port(), "--target", "monitor-log-rollout", "--request-id", requestId.toString(),
                    "--payload", "실험 경계"), stderr.toString());
            BrokerExtensionRequestData request = peer.finished.get(5, TimeUnit.SECONDS);
            assertEquals(requestId, request.requestId());
            assertEquals("monitor-log-rollout", request.target());
            assertEquals("flush-and-rollout", request.operation());
            assertEquals(2000, request.timeoutMs());
            assertArrayEquals("실험 경계".getBytes(StandardCharsets.UTF_8), Utils.toArray(request.payload()));
            assertTrue(stdout.toString().contains("status=SUCCESS"));
        }
    }

    @Test
    void brokerErrorReturnsNonzeroOverTheRealTransport() throws Exception {
        try (Peer peer = new Peer(true, Errors.CLUSTER_AUTHORIZATION_FAILED)) {
            assertEquals(1, execute(peer.port()));
            assertNotNull(peer.finished.get(5, TimeUnit.SECONDS));
            assertTrue(stderr.toString().contains("ClusterAuthorizationException"));
            assertFalse(stdout.toString().contains("status=SUCCESS"));
        }
    }

    @Test
    void unsupportedApiFailsWithoutSendingAnExtensionRequest() throws Exception {
        try (Peer peer = new Peer(false, Errors.NONE)) {
            assertEquals(1, execute(peer.port()));
            assertNull(peer.finished.get(5, TimeUnit.SECONDS));
            assertTrue(stderr.toString().contains("UnsupportedVersionException"));
        }
    }

    @Test
    void silentBrokerTimesOutAndClosesTheSocket() throws Exception {
        try (ServerSocket server = listener()) {
            CompletableFuture<Void> closed = new CompletableFuture<>();
            Thread thread = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(5000);
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    readFrame(input); // Accept ApiVersions but never respond.
                    assertEquals(-1, input.read());
                    closed.complete(null);
                } catch (Throwable error) {
                    closed.completeExceptionally(error);
                }
            }, "silent-extension-test-peer");
            thread.setDaemon(true);
            thread.start();
            long started = System.nanoTime();
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertEquals(1, execute(server.getLocalPort())));
            closed.get(5, TimeUnit.SECONDS);
            assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(1500));
            assertTrue(stderr.toString().contains("BROKER_EXTENSION_FAILED"), stderr.toString());
        }
    }

    @Test
    void missingExtensionResponseTimesOutWithoutRetrying() throws Exception {
        try (Peer peer = new Peer(true, null)) {
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertEquals(1, execute(peer.port())));
            assertNotNull(peer.finished.get(5, TimeUnit.SECONDS));
            assertTrue(stderr.toString().contains("TimeoutException"), stderr.toString());
            assertFalse(stdout.toString().contains("status=SUCCESS"));
        }
    }

    private static ServerSocket listener() throws Exception {
        ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(5000);
        return server;
    }

    private static ByteBuffer readFrame(DataInputStream input) throws Exception {
        int size = input.readInt();
        if (size <= 0 || size > 1_000_000) throw new IllegalStateException("Invalid Kafka frame size: " + size);
        byte[] frame = new byte[size];
        input.readFully(frame);
        return ByteBuffer.wrap(frame);
    }

    private static void reply(Socket socket, RequestHeader request, AbstractResponse response) throws Exception {
        ResponseHeader header = new ResponseHeader(request.correlationId(),
                request.apiKey().responseHeaderVersion(request.apiVersion()));
        ByteBuffer frame = RequestUtils.serialize(header.data(), header.headerVersion(), response.data(), request.apiVersion());
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(frame.remaining());
        output.write(Utils.toArray(frame));
        output.flush();
    }

    private static final class Peer implements AutoCloseable {
        final CompletableFuture<BrokerExtensionRequestData> finished = new CompletableFuture<>();
        final ServerSocket server;

        Peer(boolean supported, Errors error) throws Exception {
            server = listener();
            Thread thread = new Thread(() -> serve(supported, error), "broker-extension-test-peer");
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return server.getLocalPort();
        }

        private void serve(boolean supported, Errors error) {
            try (Socket socket = server.accept()) {
                socket.setSoTimeout(5000);
                DataInputStream input = new DataInputStream(socket.getInputStream());
                RequestHeader versionsHeader = RequestHeader.parse(readFrame(input));
                assertEquals(ApiKeys.API_VERSIONS, versionsHeader.apiKey());
                ApiVersionCollection versions = new ApiVersionCollection();
                versions.add(new ApiVersion().setApiKey(ApiKeys.API_VERSIONS.id).setMinVersion((short) 0)
                        .setMaxVersion(ApiKeys.API_VERSIONS.latestVersion()));
                if (supported) versions.add(new ApiVersion().setApiKey(ApiKeys.BROKER_EXTENSION.id)
                        .setMinVersion((short) 0).setMaxVersion((short) 0));
                reply(socket, versionsHeader, new ApiVersionsResponse(new ApiVersionsResponseData().setApiKeys(versions)));
                BrokerExtensionRequestData request = null;
                if (supported) {
                    ByteBuffer frame = readFrame(input);
                    RequestHeader header = RequestHeader.parse(frame);
                    assertEquals(ApiKeys.BROKER_EXTENSION, header.apiKey()); // No metadata discovery or forwarding.
                    request = new BrokerExtensionRequestData(new ByteBufferAccessor(frame), header.apiVersion());
                    if (error != null) {
                        reply(socket, header, new org.apache.kafka.common.requests.BrokerExtensionResponse(
                                new BrokerExtensionResponseData().setErrorCode(error.code()).setErrorMessage(error.message())));
                    }
                }
                assertEquals(-1, input.read()); // Client closes instead of retrying the request.
                finished.complete(request);
            } catch (Throwable failure) {
                finished.completeExceptionally(failure);
            }
        }

        @Override
        public void close() throws Exception {
            server.close();
        }
    }
}
