package org.example.apps;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ResetTransientTopicsOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Resets all transient data in a maintenance window after all transient clients have stopped. */
@Command(name = "reset-transient-topics", mixinStandardHelpOptions = true,
        description = "Reset ALL transient topic data and index state in the cluster. "
                + "Stop all transient clients first; brokers must enable transient.topic.reset.enabled.")
public final class ResetTransientTopics implements Callable<Integer> {
    @Option(names = {"-b", "--brokers", "--bootstrap-server"}, required = true,
            description = "Kafka bootstrap servers (comma-separated)")
    private String brokers;

    @Option(names = "--timeout-ms", defaultValue = "300000",
            description = "Whole reset timeout in milliseconds (default: ${DEFAULT-VALUE})")
    private int timeoutMs;

    @Option(names = "--command-config", description = "Kafka Admin/security properties file")
    private Path commandConfig;

    @Spec
    private CommandSpec spec;

    private final Function<Properties, Admin> adminFactory;

    public ResetTransientTopics() {
        this(AdminClient::create);
    }

    ResetTransientTopics(Function<Properties, Admin> adminFactory) {
        this.adminFactory = adminFactory;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ResetTransientTopics()).execute(args));
    }

    @Override
    public Integer call() {
        if (brokers.isBlank() || timeoutMs <= 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "Bootstrap servers must be non-empty and --timeout-ms must be positive");
        }
        try {
            Properties config = new Properties();
            if (commandConfig != null) {
                try (InputStream input = Files.newInputStream(commandConfig)) {
                    config.load(input);
                }
            }
            config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            config.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "transient-topic-reset");
            spec.commandLine().getOut().printf("RESET_STARTED bootstrapServers=%s timeoutMs=%d scope=cluster%n",
                    brokers, timeoutMs);
            spec.commandLine().getOut().flush();
            reset(config);
            spec.commandLine().getOut().println("RESET_COMPLETE status=SUCCESS scope=cluster");
            spec.commandLine().getOut().flush();
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reportFailure(e);
            return 130;
        } catch (Exception e) {
            reportFailure(e);
            return 1;
        }
    }

    private void reset(Properties config) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        Admin admin = adminFactory.apply(config);
        try {
            admin.resetTransientTopics(new ResetTransientTopicsOptions().timeoutMs(timeoutMs))
                    .all().get(Math.max(1L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } finally {
            long closeNanos = Math.max(0L, Math.min(TimeUnit.SECONDS.toNanos(5), deadline - System.nanoTime()));
            admin.close(Duration.ofNanos(closeNanos));
        }
    }

    private void reportFailure(Exception failure) {
        Throwable cause = failure;
        while (cause instanceof ExecutionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        String detail = String.valueOf(cause.getMessage()).replace('\n', ' ').replace('\r', ' ');
        spec.commandLine().getErr().printf("RESET_FAILED type=%s detail=%s%n",
                cause.getClass().getSimpleName(), detail);
        spec.commandLine().getErr().flush();
    }
}
