import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaJmxProducerBatchDaemon {
    private static final long DEFAULT_SCAN_INTERVAL_MS = 1_000L;
    private static final long DEFAULT_INACTIVITY_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long DEFAULT_INACTIVITY_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long DEFAULT_SCAN_LOG_INTERVAL_MS = 60_000L;
    private static final ThreadLocal<SimpleDateFormat> KST_DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        format.setTimeZone(java.util.TimeZone.getTimeZone("Asia/Seoul"));
        return format;
    });

    public enum DeletionMode {
        IMMEDIATE,
        BATCH
    }

    public static class MetricsCsvWriter {
        private final String filePath;
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        public MetricsCsvWriter(String filePath) {
            this.filePath = filePath;
            initFileHeader();
        }

        private synchronized void initFileHeader() {
            File file = new File(filePath);
            if (!file.exists() || file.length() == 0) {
                try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
                    pw.println("DATETIME,UNIX_TIMESTAMP_MS,OPERATION,CURRENT_MS,AVG_MS,MEDIAN_MS,P90_MS,P99_MS,SAMPLE_COUNT,DETAILS");
                } catch (IOException e) {
                    System.err.println("[MetricsWriter Error] Failed to create header: " + e.getMessage());
                }
            }
        }

        public synchronized void writeMetric(
                long nowMs,
                String operation,
                double current,
                double avg,
                double median,
                double p90,
                double p99,
                int count,
                String details
        ) {
            String dateStr = dateFormat.format(new Date(nowMs));
            String csvLine = String.format(
                    Locale.US,
                    "%s,%d,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%s",
                    dateStr,
                    nowMs,
                    operation,
                    current,
                    avg,
                    median,
                    p90,
                    p99,
                    count,
                    csvEscape(details)
            );

            try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))) {
                pw.println(csvLine);
            } catch (IOException e) {
                System.err.println("[MetricsWriter Error] Failed to write metric: " + e.getMessage());
            }

            System.out.printf(
                    Locale.US,
                    "[METRICS] %s | Unix: %d | Op: %-16s | Cur: %7.3fms | Avg: %7.3fms | P50: %7.3fms | P90: %7.3fms | P99: %7.3fms | N: %d%s%n",
                    dateStr,
                    nowMs,
                    operation,
                    current,
                    avg,
                    median,
                    p90,
                    p99,
                    count,
                    details == null || details.isEmpty() ? "" : " | Details: " + details
            );
        }

        private String csvEscape(String value) {
            if (value == null || value.isEmpty()) {
                return "";
            }
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
    }

    public static class PerformanceTracker {
        private final String operationName;
        private final int maxSamples;
        private final List<Double> samples = new ArrayList<>();
        private final MetricsCsvWriter csvWriter;
        private double lastDurationMs = 0.0;

        public PerformanceTracker(String operationName, int maxSamples, MetricsCsvWriter csvWriter) {
            this.operationName = operationName;
            this.maxSamples = maxSamples;
            this.csvWriter = csvWriter;
        }

        public synchronized void recordNano(long durationNano) {
            recordNano(durationNano, "");
        }

        public synchronized void recordNano(long durationNano, String details) {
            double durationMs = durationNano / 1_000_000.0;
            lastDurationMs = durationMs;

            if (samples.size() >= maxSamples) {
                samples.remove(0);
            }
            samples.add(durationMs);

            publishMetric(details);
        }

        private void publishMetric(String details) {
            if (samples.isEmpty()) {
                return;
            }

            long nowMs = System.currentTimeMillis();
            List<Double> sorted = new ArrayList<>(samples);
            Collections.sort(sorted);

            double sum = 0.0;
            for (double sample : sorted) {
                sum += sample;
            }

            int size = sorted.size();
            csvWriter.writeMetric(
                    nowMs,
                    operationName,
                    lastDurationMs,
                    sum / size,
                    getPercentile(sorted, 50.0),
                    getPercentile(sorted, 90.0),
                    getPercentile(sorted, 99.0),
                    size,
                    details
            );
        }

        private double getPercentile(List<Double> sorted, double percentile) {
            int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
            index = Math.max(0, Math.min(index, sorted.size() - 1));
            return sorted.get(index);
        }
    }

    public static class TopicContext {
        private final String topicName;
        private final long firstSeenAtMs;
        private long lastObservedAtMs;
        private long lastChangedAtMs;
        private long lastOffset;
        private boolean offsetInitialized;

        public TopicContext(String topicName, long nowMs) {
            this.topicName = topicName;
            this.firstSeenAtMs = nowMs;
            this.lastObservedAtMs = nowMs;
            this.lastChangedAtMs = nowMs;
            this.lastOffset = 0L;
            this.offsetInitialized = false;
        }

        public synchronized boolean updateOffset(long currentOffset, long nowMs) {
            lastObservedAtMs = nowMs;

            if (!offsetInitialized) {
                lastOffset = currentOffset;
                lastChangedAtMs = nowMs;
                offsetInitialized = true;
                return false;
            }

            if (currentOffset > lastOffset) {
                lastOffset = currentOffset;
                lastChangedAtMs = nowMs;
                return true;
            }

            return false;
        }

        public synchronized boolean isInactive(long nowMs, long inactivityThresholdMs) {
            return offsetInitialized && nowMs - lastChangedAtMs >= inactivityThresholdMs;
        }

        public synchronized long getLastOffset() {
            return lastOffset;
        }

        public synchronized long getFirstSeenAtMs() {
            return firstSeenAtMs;
        }

        public synchronized long getLastObservedAtMs() {
            return lastObservedAtMs;
        }

        public synchronized long getLastChangedAtMs() {
            return lastChangedAtMs;
        }

        public String getTopicName() {
            return topicName;
        }
    }

    private static class TopicOffsetSnapshot {
        private final Map<String, Long> offsetsByTopic;
        private final Set<String> observedTopicNames;
        private final int mbeanCount;
        private final int nonInternalMBeanCount;
        private final int observedTopicCount;
        private final int attributeReadAttempts;
        private final int attributeReadFailures;
        private final long queryNamesDurationMs;
        private final long attributeReadDurationMs;

        private TopicOffsetSnapshot(
                Map<String, Long> offsetsByTopic,
                Set<String> observedTopicNames,
                int mbeanCount,
                int nonInternalMBeanCount,
                int observedTopicCount,
                int attributeReadAttempts,
                int attributeReadFailures,
                long queryNamesDurationMs,
                long attributeReadDurationMs
        ) {
            this.offsetsByTopic = offsetsByTopic;
            this.observedTopicNames = observedTopicNames;
            this.mbeanCount = mbeanCount;
            this.nonInternalMBeanCount = nonInternalMBeanCount;
            this.observedTopicCount = observedTopicCount;
            this.attributeReadAttempts = attributeReadAttempts;
            this.attributeReadFailures = attributeReadFailures;
            this.queryNamesDurationMs = queryNamesDurationMs;
            this.attributeReadDurationMs = attributeReadDurationMs;
        }
    }

    private final String jmxHost;
    private final int jmxPort;
    private final String bootstrapServers;
    private final long scanIntervalMs;
    private final long inactivityCheckIntervalMs;
    private final long inactivityThresholdMs;
    private final long scanLogIntervalMs;
    private final DeletionMode deletionMode;

    private final ConcurrentHashMap<String, TopicContext> topicRegistry = new ConcurrentHashMap<>();
    private final Set<String> deletionInFlightTopics = ConcurrentHashMap.newKeySet();
    private final Set<String> confirmedDeletedTopics = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<List<String>> deleteQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger deleteQueuedTopicCount = new AtomicInteger(0);
    private final AtomicInteger deletingTopicCount = new AtomicInteger(0);

    private final MetricsCsvWriter csvWriter;
    private final PerformanceTracker deletionTracker;

    private JMXConnector jmxConnector;
    private MBeanServerConnection mbsc;
    private AdminClient adminClient;
    private Thread lookupThread;
    private Thread deletionThread;
    private volatile int lastScanMBeanCount = 0;
    private volatile int lastScanNonInternalMBeanCount = 0;
    private volatile int lastScanObservedTopicCount = 0;
    private volatile int lastScanOffsetReadTopicCount = 0;
    private volatile int lastScanOffsetChangedTopics = 0;
    private volatile int lastScanMissingMBeanRemovals = 0;
    private volatile int lastScanDiscoveredTopics = 0;
    private volatile int lastScanRegisteredTopicsStart = 0;
    private volatile int lastScanRegisteredTopicsEnd = 0;
    private volatile int lastScanAttributeReadAttempts = 0;
    private volatile int lastScanAttributeReadFailures = 0;
    private volatile long lastScanQueryNamesDurationMs = 0L;
    private volatile long lastScanAttributeReadDurationMs = 0L;
    private final AtomicInteger registeredTopicsSinceLastScan = new AtomicInteger(0);
    private final AtomicInteger unregisteredLogEndOffsetMBeansSinceLastScan = new AtomicInteger(0);

    public KafkaJmxProducerBatchDaemon(
            String jmxHost,
            int jmxPort,
            String bootstrapServers,
            long scanIntervalMs,
            long inactivityCheckIntervalMs,
            long inactivityThresholdMs,
            long scanLogIntervalMs,
            DeletionMode deletionMode,
            String csvFilePath
    ) {
        this.jmxHost = jmxHost;
        this.jmxPort = jmxPort;
        this.bootstrapServers = bootstrapServers;
        this.scanIntervalMs = positiveOrDefault(scanIntervalMs, DEFAULT_SCAN_INTERVAL_MS);
        this.inactivityCheckIntervalMs = positiveOrDefault(
                inactivityCheckIntervalMs,
                DEFAULT_INACTIVITY_CHECK_INTERVAL_MS
        );
        this.inactivityThresholdMs = positiveOrDefault(inactivityThresholdMs, DEFAULT_INACTIVITY_THRESHOLD_MS);
        this.scanLogIntervalMs = positiveOrDefault(scanLogIntervalMs, DEFAULT_SCAN_LOG_INTERVAL_MS);
        this.deletionMode = deletionMode;

        this.csvWriter = new MetricsCsvWriter(csvFilePath);
        this.deletionTracker = new PerformanceTracker("DELETION", 1_000, csvWriter);
    }

    public void start() throws Exception {
        connectJmx();
        initAdminClient();
        registerJmxNotificationListener();
        populateExistingTopics();
        startDeletionThread();
        startLookupThread();
    }

    private void connectJmx() throws IOException {
        String serviceUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", jmxHost, jmxPort);
        jmxConnector = JMXConnectorFactory.connect(new JMXServiceURL(serviceUrl), null);
        mbsc = jmxConnector.getMBeanServerConnection();
        System.out.printf("[Daemon] JMX connected (%s:%d)%n", jmxHost, jmxPort);
    }

    private void initAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(props);
        System.out.println("[Daemon] Kafka AdminClient initialized");
    }

    private void registerJmxNotificationListener() throws Exception {
        ObjectName mbeanServerDelegate = new ObjectName("JMImplementation:type=MBeanServerDelegate");

        NotificationListener listener = (notification, handback) -> {
            if (!(notification instanceof MBeanServerNotification)) {
                return;
            }

            MBeanServerNotification mbs = (MBeanServerNotification) notification;
            String notificationType = mbs.getType();
            if (!MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notificationType)
                    && !MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(notificationType)) {
                return;
            }

            ObjectName mbeanName = mbs.getMBeanName();
            if (!isLogEndOffsetMBean(mbeanName)) {
                return;
            }

            if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notificationType)) {
                registerTopicFromMBean(mbeanName);
            } else {
                recordLogEndOffsetMBeanUnregistration(mbeanName);
            }
        };

        mbsc.addNotificationListener(mbeanServerDelegate, listener, null, null);
        System.out.println("[Daemon] JMX MBean registration listener installed");
    }

    private void populateExistingTopics() throws Exception {
        ObjectName pattern = new ObjectName("kafka.log:type=Log,name=LogEndOffset,topic=*,partition=*");
        Set<ObjectName> mbeans = mbsc.queryNames(pattern, null);

        for (ObjectName mbean : mbeans) {
            registerTopicFromMBean(mbean, false);
        }

        System.out.printf(
                "[Daemon] Initial scan complete. LogEndOffset MBeans: %d, non-internal topics registered: %d%n",
                mbeans.size(),
                topicRegistry.size()
        );
    }

    private void registerTopicFromMBean(ObjectName mbean) {
        registerTopicFromMBean(mbean, true);
    }

    private void registerTopicFromMBean(ObjectName mbean, boolean emitMetricAndLog) {
        String topic = extractTopic(mbean);
        if (topic == null || isInternalTopic(topic)) {
            return;
        }
        if (deletionInFlightTopics.contains(topic) || confirmedDeletedTopics.contains(topic)) {
            return;
        }

        TopicContext created = new TopicContext(topic, System.currentTimeMillis());
        TopicContext existing = topicRegistry.putIfAbsent(topic, created);
        if (existing == null && emitMetricAndLog) {
            registeredTopicsSinceLastScan.incrementAndGet();
        }
    }

    private void recordLogEndOffsetMBeanUnregistration(ObjectName mbean) {
        String topic = extractTopic(mbean);
        if (topic == null || isInternalTopic(topic)) {
            return;
        }

        confirmedDeletedTopics.remove(topic);
        unregisteredLogEndOffsetMBeansSinceLastScan.incrementAndGet();
    }

    private boolean isLogEndOffsetMBean(ObjectName objectName) {
        return "kafka.log".equals(objectName.getDomain())
                && "Log".equals(objectName.getKeyProperty("type"))
                && "LogEndOffset".equals(objectName.getKeyProperty("name"));
    }

    private String extractTopic(ObjectName objectName) {
        String topic = objectName.getKeyProperty("topic");
        if (topic == null) {
            return null;
        }

        try {
            return ObjectName.unquote(topic);
        } catch (IllegalArgumentException ignored) {
            return topic;
        }
    }

    private boolean isInternalTopic(String topic) {
        // 접두어는 관리 범위만 제한한다. 실제 삭제 여부는 마지막 오프셋 증가 이후의 비활성 시간으로 판정한다.
        String scope = System.getenv("DELETE_TOPIC_PREFIX");
        return topic.startsWith("__") || (scope != null && !scope.isEmpty() && !topic.startsWith(scope));
    }

    private void startLookupThread() {
        lookupThread = new Thread(() -> {
            System.out.printf(
                    "[Thread-1: Lookup] started. scanIntervalMs=%d, scanLogIntervalMs=%d, inactivityCheckIntervalMs=%d, inactivityThresholdMs=%d, deletionMode=%s%n",
                    scanIntervalMs,
                    scanLogIntervalMs,
                    inactivityCheckIntervalMs,
                    inactivityThresholdMs,
                    deletionMode
            );

            long nextInactiveCheckAtMs = System.currentTimeMillis() + inactivityCheckIntervalMs;
            long nextScanLogAtMs = 0L;

            while (running.get()) {
                long loopStartMs = System.currentTimeMillis();

                try {
                    long nowMs = System.currentTimeMillis();
                    int newRegistrations = registeredTopicsSinceLastScan.getAndSet(0);
                    int mbeanUnregistrations = unregisteredLogEndOffsetMBeansSinceLastScan.getAndSet(0);

                    int inactiveQueued = 0;
                    boolean inactiveCheckRan = false;
                    if (nowMs >= nextInactiveCheckAtMs) {
                        syncTopicMBeans(nowMs, true);
                        inactiveQueued = scanAndEnqueueInactiveTopics(nowMs);
                        inactiveCheckRan = true;
                        nextInactiveCheckAtMs = nowMs + inactivityCheckIntervalMs;
                    } else {
                        syncTopicMBeans(nowMs, false);
                    }

                    long elapsedMs = System.currentTimeMillis() - loopStartMs;
                    if (nowMs >= nextScanLogAtMs) {
                        String inactiveSummary = inactiveCheckRan
                                ? "checked(q:" + inactiveQueued + ")"
                                : "skip";
                        String attributeFailureSummary = lastScanAttributeReadFailures > 0
                                ? String.format(
                                        Locale.US,
                                        " attrFail=%d/%d",
                                        lastScanAttributeReadFailures,
                                        lastScanAttributeReadAttempts
                                )
                                : "";
                        System.out.printf(
                                Locale.US,
                                "[%s] scanMs=%d reg=%d->%d del=%d(q:%d run:%d b:%d) jmx=%d(userMb:%d topics:%d read:%d ms:%d+%d) evt=+%d scan+%d unreg:%d chg:%d miss:%d inactive=%s%s%n",
                                formatKst(nowMs),
                                elapsedMs,
                                lastScanRegisteredTopicsStart,
                                lastScanRegisteredTopicsEnd,
                                deletionInFlightTopics.size(),
                                deleteQueuedTopicCount.get(),
                                deletingTopicCount.get(),
                                deleteQueue.size(),
                                lastScanMBeanCount,
                                lastScanNonInternalMBeanCount,
                                lastScanObservedTopicCount,
                                lastScanOffsetReadTopicCount,
                                lastScanQueryNamesDurationMs,
                                lastScanAttributeReadDurationMs,
                                newRegistrations,
                                lastScanDiscoveredTopics,
                                mbeanUnregistrations,
                                lastScanOffsetChangedTopics,
                                lastScanMissingMBeanRemovals,
                                inactiveSummary,
                                attributeFailureSummary
                        );
                        nextScanLogAtMs = nowMs + scanLogIntervalMs;
                    }
                } catch (Exception e) {
                    System.err.println("[Lookup Thread Error] " + e.getMessage());
                }

                sleepUntilNextScan(loopStartMs);
            }

            System.out.println("[Thread-1: Lookup] stopped");
        }, "Lookup-TopicMonitor");

        lookupThread.setDaemon(true);
        lookupThread.start();
    }

    private void syncTopicMBeans(long nowMs, boolean readOffsets) throws Exception {
        int registeredTopicsStart = topicRegistry.size();
        Set<String> scanStartTopics = new HashSet<>(topicRegistry.keySet());
        scanStartTopics.removeAll(deletionInFlightTopics);
        TopicOffsetSnapshot snapshot = queryAllTopicOffsets(scanStartTopics, readOffsets);
        Map<String, Long> offsetsByTopic = snapshot.offsetsByTopic;
        lastScanMBeanCount = snapshot.mbeanCount;
        lastScanNonInternalMBeanCount = snapshot.nonInternalMBeanCount;
        lastScanObservedTopicCount = snapshot.observedTopicCount;
        lastScanOffsetReadTopicCount = snapshot.offsetsByTopic.size();
        lastScanAttributeReadAttempts = snapshot.attributeReadAttempts;
        lastScanAttributeReadFailures = snapshot.attributeReadFailures;
        lastScanQueryNamesDurationMs = snapshot.queryNamesDurationMs;
        lastScanAttributeReadDurationMs = snapshot.attributeReadDurationMs;
        lastScanRegisteredTopicsStart = registeredTopicsStart;
        int missingMBeanRemovals = 0;
        int offsetChangedTopics = 0;

        for (String topic : scanStartTopics) {
            TopicContext context = topicRegistry.get(topic);
            if (context == null || deletionInFlightTopics.contains(topic)) {
                continue;
            }

            if (!snapshot.observedTopicNames.contains(topic)) {
                if (topicRegistry.remove(topic, context)) {
                    missingMBeanRemovals++;
                }
                continue;
            }

            if (!readOffsets) {
                continue;
            }

            Long totalOffset = offsetsByTopic.get(topic);
            if (totalOffset == null) {
                continue;
            }

            if (context.updateOffset(totalOffset, nowMs)) {
                offsetChangedTopics++;
            }
        }

        lastScanMissingMBeanRemovals = missingMBeanRemovals;
        lastScanOffsetChangedTopics = offsetChangedTopics;
        lastScanDiscoveredTopics = registerObservedTopicsFromScan(snapshot.observedTopicNames, nowMs);
        lastScanRegisteredTopicsEnd = topicRegistry.size();
    }

    private int registerObservedTopicsFromScan(Set<String> observedTopics, long nowMs) {
        int discoveredTopics = 0;
        for (String topic : observedTopics) {
            if (deletionInFlightTopics.contains(topic) || confirmedDeletedTopics.contains(topic)) {
                continue;
            }

            TopicContext created = new TopicContext(topic, nowMs);
            if (topicRegistry.putIfAbsent(topic, created) == null) {
                discoveredTopics++;
            }
        }
        return discoveredTopics;
    }

    private TopicOffsetSnapshot queryAllTopicOffsets(Set<String> targetTopics, boolean readOffsets) throws Exception {
        ObjectName pattern = new ObjectName("kafka.log:type=Log,name=LogEndOffset,topic=*,partition=*");
        long queryNamesStartNano = System.nanoTime();
        Set<ObjectName> mbeans = mbsc.queryNames(pattern, null);
        long queryNamesDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - queryNamesStartNano);
        Map<String, Long> offsetsByTopic = new HashMap<>();
        Set<String> observedTopics = new HashSet<>();
        int nonInternalMBeanCount = 0;
        int attributeReadAttempts = 0;
        int attributeReadFailures = 0;
        long attributeReadDurationNano = 0L;

        for (ObjectName mbean : mbeans) {
            String topic = extractTopic(mbean);
            if (topic == null || isInternalTopic(topic)) {
                continue;
            }

            nonInternalMBeanCount++;
            observedTopics.add(topic);
            if (!readOffsets || !targetTopics.contains(topic)) {
                continue;
            }

            attributeReadAttempts++;
            long attributeReadStartNano = System.nanoTime();
            try {
                Object val = mbsc.getAttribute(mbean, "Value");
                if (val instanceof Number) {
                    offsetsByTopic.merge(topic, ((Number) val).longValue(), Long::sum);
                }
            } catch (javax.management.InstanceNotFoundException ignored) {
                attributeReadFailures++;
            } finally {
                attributeReadDurationNano += System.nanoTime() - attributeReadStartNano;
            }
        }
        long attributeReadDurationMs = TimeUnit.NANOSECONDS.toMillis(attributeReadDurationNano);

        return new TopicOffsetSnapshot(
                offsetsByTopic,
                observedTopics,
                mbeans.size(),
                nonInternalMBeanCount,
                observedTopics.size(),
                attributeReadAttempts,
                attributeReadFailures,
                queryNamesDurationMs,
                attributeReadDurationMs
        );
    }

    private int scanAndEnqueueInactiveTopics(long nowMs) {
        if (deletionMode == DeletionMode.IMMEDIATE) {
            int queued = 0;
            for (Map.Entry<String, TopicContext> entry : topicRegistry.entrySet()) {
                String topic = entry.getKey();
                TopicContext ctx = entry.getValue();
                if (ctx.isInactive(nowMs, inactivityThresholdMs) && deletionInFlightTopics.add(topic)) {
                    enqueueTopicsForDeletion(Collections.singletonList(topic));
                    queued++;
                }
            }
            return queued;
        }

        List<String> batchRequest = new ArrayList<>();
        for (Map.Entry<String, TopicContext> entry : topicRegistry.entrySet()) {
            String topic = entry.getKey();
            TopicContext ctx = entry.getValue();
            if (ctx.isInactive(nowMs, inactivityThresholdMs) && deletionInFlightTopics.add(topic)) {
                batchRequest.add(topic);
            }
        }

        if (!batchRequest.isEmpty()) {
            enqueueTopicsForDeletion(batchRequest);
        }
        return batchRequest.size();
    }

    private void enqueueTopicsForDeletion(List<String> topics) {
        if (topics.isEmpty()) {
            return;
        }

        deleteQueue.offer(topics);
        deleteQueuedTopicCount.addAndGet(topics.size());
    }

    private void startDeletionThread() {
        deletionThread = new Thread(() -> {
            System.out.println("[Thread-2: Deleter] started");

            while (running.get()) {
                List<String> topicsToDelete = null;
                try {
                    topicsToDelete = deleteQueue.take();
                    if (topicsToDelete.isEmpty()) {
                        continue;
                    }
                    deleteQueuedTopicCount.addAndGet(-topicsToDelete.size());
                    deletingTopicCount.addAndGet(topicsToDelete.size());

                    long startNano = System.nanoTime();
                    DeleteTopicsResult result = adminClient.deleteTopics(topicsToDelete);
                    Map<String, Long> topicDurationsMs = waitForTopicDeletes(result, topicsToDelete, startNano);
                    int registryRemoved = removeConfirmedDeletedTopics(topicDurationsMs.keySet());
                    long totalDurationNano = System.nanoTime() - startNano;
                    deletionTracker.recordNano(
                            totalDurationNano,
                            "topicCount=" + topicsToDelete.size()
                    );

                    System.out.printf(
                            "[Deleter] deleted topicCount=%d registryRemoved=%d totalDurationMs=%.3f avgTopicCompletionMs=%.3f minTopicCompletionMs=%d maxTopicCompletionMs=%d%n",
                            topicsToDelete.size(),
                            registryRemoved,
                            totalDurationNano / 1_000_000.0,
                            averageDuration(topicDurationsMs),
                            minDuration(topicDurationsMs),
                            maxDuration(topicDurationsMs)
                    );
                    for (Map.Entry<String, Long> entry : topicDurationsMs.entrySet()) {
                        System.out.printf(
                                "[DeleterTopic] topic=%s completionMs=%d%n",
                                entry.getKey(),
                                entry.getValue()
                        );
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("[Deleter Thread Error] delete failed: " + e.getMessage());
                    clearDeleteInFlight(topicsToDelete);
                } finally {
                    if (topicsToDelete != null && !topicsToDelete.isEmpty()) {
                        deletingTopicCount.addAndGet(-topicsToDelete.size());
                    }
                }
            }

            System.out.println("[Thread-2: Deleter] stopped");
        }, "Deleter-TopicDelete");

        deletionThread.setDaemon(true);
        deletionThread.start();
    }

    private int removeConfirmedDeletedTopics(Iterable<String> topics) {
        int removed = 0;
        for (String topic : topics) {
            if (topicRegistry.remove(topic) != null) {
                removed++;
            }
            deletionInFlightTopics.remove(topic);
            confirmedDeletedTopics.add(topic);
        }
        return removed;
    }

    private void clearDeleteInFlight(List<String> topics) {
        if (topics == null) {
            return;
        }

        for (String topic : topics) {
            deletionInFlightTopics.remove(topic);
        }
    }

    private Map<String, Long> waitForTopicDeletes(
            DeleteTopicsResult result,
            List<String> topicsToDelete,
            long startNano
    ) throws Exception {
        Map<String, Long> topicDurationsMs = new LinkedHashMap<>();
        Map<String, ?> futures = topicDeleteFutures(result);

        for (String topic : topicsToDelete) {
            Object future = futures.get(topic);
            if (future == null) {
                throw new IllegalStateException("DeleteTopicsResult has no future for topic: " + topic);
            }

            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
            long remainingMs = Math.max(1L, 10_000L - elapsedMs);
            future.getClass()
                    .getMethod("get", long.class, TimeUnit.class)
                    .invoke(future, remainingMs, TimeUnit.MILLISECONDS);
            topicDurationsMs.put(topic, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
        }

        return topicDurationsMs;
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> topicDeleteFutures(DeleteTopicsResult result) throws Exception {
        try {
            return (Map<String, ?>) DeleteTopicsResult.class
                    .getMethod("topicNameValues")
                    .invoke(result);
        } catch (NoSuchMethodException ignored) {
            return (Map<String, ?>) DeleteTopicsResult.class
                    .getMethod("values")
                    .invoke(result);
        }
    }

    private double averageDuration(Map<String, Long> durationsMs) {
        if (durationsMs.isEmpty()) {
            return 0.0;
        }

        long sum = 0L;
        for (long duration : durationsMs.values()) {
            sum += duration;
        }
        return sum / (double) durationsMs.size();
    }

    private long minDuration(Map<String, Long> durationsMs) {
        if (durationsMs.isEmpty()) {
            return 0L;
        }

        long min = Long.MAX_VALUE;
        for (long duration : durationsMs.values()) {
            min = Math.min(min, duration);
        }
        return min;
    }

    private long maxDuration(Map<String, Long> durationsMs) {
        long max = 0L;
        for (long duration : durationsMs.values()) {
            max = Math.max(max, duration);
        }
        return max;
    }

    private void sleepUntilNextScan(long loopStartMs) {
        long sleepMs = calculateNextSleepMs(loopStartMs);

        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private long calculateNextSleepMs(long loopStartMs) {
        long elapsedMs = System.currentTimeMillis() - loopStartMs;
        return Math.max(1L, scanIntervalMs - elapsedMs);
    }

    public void stop() {
        running.set(false);

        if (lookupThread != null) {
            lookupThread.interrupt();
        }
        if (deletionThread != null) {
            deletionThread.interrupt();
        }
        if (adminClient != null) {
            adminClient.close();
        }
        if (jmxConnector != null) {
            try {
                jmxConnector.close();
            } catch (IOException ignored) {
            }
        }

        System.out.println("[Daemon] stopped");
    }

    private static long positiveOrDefault(long value, long defaultValue) {
        return value > 0L ? value : defaultValue;
    }

    private static String formatKst(long timestampMs) {
        return KST_DATE_FORMAT.get().format(new Date(timestampMs));
    }

    public static void main(String[] args) throws Exception {
        // 생산 실험 사이의 정리 요청이면 JMX 상시 감시 대신 제한된 일회성 삭제를 수행한다.
        if ("true".equals(System.getenv("CLEANUP_ONCE"))) {
            cleanupExperimentTopics(args);
            return;
        }
        String jmxHost = "127.0.0.1";
        int jmxPort = 9999;
        String bootstrapServers = "127.0.0.1:9092";
        long scanIntervalMs = DEFAULT_SCAN_INTERVAL_MS;
        long inactivityCheckIntervalMs = DEFAULT_INACTIVITY_CHECK_INTERVAL_MS;
        long inactivityThresholdMs = DEFAULT_INACTIVITY_THRESHOLD_MS;
        long scanLogIntervalMs = DEFAULT_SCAN_LOG_INTERVAL_MS;
        DeletionMode selectedMode = DeletionMode.BATCH;
        String csvFilePath = "topic_metrics.csv";

        if (args.length > 0) {
            jmxHost = args[0];
        }
        if (args.length > 1) {
            jmxPort = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            bootstrapServers = args[2];
        }
        if (args.length > 3) {
            scanIntervalMs = Long.parseLong(args[3]);
        }
        if (args.length > 4) {
            inactivityCheckIntervalMs = Long.parseLong(args[4]);
        }
        if (args.length > 5) {
            selectedMode = DeletionMode.valueOf(args[5].toUpperCase(Locale.ROOT));
        }
        if (args.length > 6) {
            csvFilePath = args[6];
        }
        if (args.length > 7) {
            inactivityThresholdMs = Long.parseLong(args[7]);
        }
        if (args.length > 8) {
            scanLogIntervalMs = Long.parseLong(args[8]);
        }

        System.out.println("==================================================");
        System.out.println("   Kafka JMX Inactive Topic Delete Daemon");
        System.out.println("==================================================");
        System.out.printf(" - JMX                  : %s:%d%n", jmxHost, jmxPort);
        System.out.printf(" - Bootstrap Servers    : %s%n", bootstrapServers);
        System.out.printf(" - Scan Interval        : %d ms%n", positiveOrDefault(scanIntervalMs, DEFAULT_SCAN_INTERVAL_MS));
        System.out.printf(" - Scan Log Interval    : %d ms%n", positiveOrDefault(scanLogIntervalMs, DEFAULT_SCAN_LOG_INTERVAL_MS));
        System.out.printf(" - Inactive Check Every : %d ms%n", positiveOrDefault(inactivityCheckIntervalMs, DEFAULT_INACTIVITY_CHECK_INTERVAL_MS));
        System.out.printf(" - Inactive Threshold   : %d ms%n", positiveOrDefault(inactivityThresholdMs, DEFAULT_INACTIVITY_THRESHOLD_MS));
        System.out.printf(" - Deletion Mode        : %s%n", selectedMode);
        System.out.printf(" - CSV Output File      : %s%n", csvFilePath);
        System.out.println("==================================================");

        KafkaJmxProducerBatchDaemon daemon = new KafkaJmxProducerBatchDaemon(
                jmxHost,
                jmxPort,
                bootstrapServers,
                scanIntervalMs,
                inactivityCheckIntervalMs,
                inactivityThresholdMs,
                scanLogIntervalMs,
                selectedMode,
                csvFilePath
        );

        Runtime.getRuntime().addShutdownHook(new Thread(daemon::stop));
        daemon.start();
        // JMX 연결 및 초기 스캔 성공 이후에만 생산기를 시작할 수 있도록 알린다.
        System.out.println("[Daemon] READY");
        Thread.currentThread().join();
    }

    // delete.sh가 전달하는 브로커 인수와 실험 전용 접두어로 삭제 범위를 제한한다.
    private static void cleanupExperimentTopics(String[] args) throws Exception {
        String topicPrefix = System.getenv("CLEANUP_TOPIC_PREFIX");
        if (topicPrefix == null || topicPrefix.trim().isEmpty() || topicPrefix.startsWith("__")) {
            throw new IllegalArgumentException("CLEANUP_TOPIC_PREFIX must be a non-empty experiment prefix");
        }
        if (args.length < 3) {
            throw new IllegalArgumentException("Missing bootstrap servers from delete.sh");
        }
        String timeoutValue = System.getenv("CLEANUP_TIMEOUT_SECONDS");
        long timeoutSeconds = timeoutValue == null ? 300L : Long.parseLong(timeoutValue);
        if (timeoutSeconds <= 0 || timeoutSeconds > 86400L) {
            throw new IllegalArgumentException("CLEANUP_TIMEOUT_SECONDS must be between 1 and 86400");
        }
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args[2]);
        String first = System.getenv("CLEANUP_START_INDEX");
        String countValue = System.getenv("CLEANUP_TOPIC_COUNT");
        boolean bounded = first != null && !first.isEmpty();
        long start = bounded ? Long.parseLong(first) : 0L;
        long count = bounded ? Long.parseLong(countValue) : 0L;
        if (bounded && (start < 0 || count < 1 || start > Long.MAX_VALUE - count)) {
            throw new IllegalArgumentException("Invalid cleanup topic index range");
        }
        java.util.function.Predicate<String> selected = name -> {
            if (name.startsWith("__") || !name.startsWith(topicPrefix)) return false;
            if (!bounded) return true;
            String suffix = name.substring(topicPrefix.length());
            try {
                long index = Long.parseLong(suffix);
                return suffix.equals(Long.toString(index)) && index >= start && index < start + count;
            } catch (NumberFormatException e) { return false; }
        };
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        AdminClient client = AdminClient.create(props);
        try {
            // 클러스터 전체 목록에서 대상을 확인하고 비동기 삭제 후 사라질 때까지 기다린다.
            Set<String> targets = new HashSet<>();
            System.out.printf("[Cleanup] phase=LISTING prefix=%s%n", topicPrefix);
            for (String name : client.listTopics().names().get(cleanupRemainingMs(deadline), TimeUnit.MILLISECONDS)) {
                if (selected.test(name)) {
                    targets.add(name);
                }
            }
            System.out.printf("[Cleanup] prefix=%s topics=%d%n", topicPrefix, targets.size());
            // Controller의 단일 요청 처리 한도를 넘지 않도록 cleanup만 순차 분할한다.
            // 실험 중 IMMEDIATE/BATCH 삭제 방식은 변경하지 않는다.
            deleteCleanupBatches(targets, deadline, (batch, remainingMs) ->
                    client.deleteTopics(batch, new org.apache.kafka.clients.admin.DeleteTopicsOptions()
                            .timeoutMs((int) Math.min(Integer.MAX_VALUE, remainingMs))).topicNameValues());
            System.out.println("[Cleanup] phase=VERIFY; checking topic disappearance");
            while (true) {
                int remaining = 0;
                for (String name : client.listTopics().names().get(cleanupRemainingMs(deadline), TimeUnit.MILLISECONDS)) {
                    if (selected.test(name)) {
                        remaining++;
                    }
                }
                System.out.printf("[Cleanup] phase=VERIFY initialTargets=%d remaining=%d timeoutRemainingMs=%d%n",
                        targets.size(), remaining, Math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                if (remaining == 0) {
                    System.out.println("[Cleanup] Experiment topics no longer listed; cleanup complete.");
                    return;
                }
                Thread.sleep(Math.min(1000L, cleanupRemainingMs(deadline)));
            }
        } finally {
            // 실패나 시간 초과에도 연결을 닫고 호출자에게 실패를 전달한다.
            client.close(java.time.Duration.ofSeconds(5));
        }
    }

    // 18,000개 토픽은 1,000개씩 18개 요청으로 정리한다. 실제 완료는 위의 목록 검증으로 판단한다.
    static final int CLEANUP_DELETE_BATCH_SIZE = 1000;

    @FunctionalInterface
    interface CleanupDeleteRequest {
        Map<String, org.apache.kafka.common.KafkaFuture<Void>> delete(List<String> topics, long remainingMs);
    }

    static void deleteCleanupBatches(Set<String> targets, long deadline, CleanupDeleteRequest request)
            throws Exception {
        List<String> ordered = new ArrayList<>(targets);
        Collections.sort(ordered); // 재현 가능한 요청 순서와 로그를 유지한다.
        int batchCount = (ordered.size() + CLEANUP_DELETE_BATCH_SIZE - 1) / CLEANUP_DELETE_BATCH_SIZE;
        for (int offset = 0, batchIndex = 1; offset < ordered.size();
                offset += CLEANUP_DELETE_BATCH_SIZE, batchIndex++) {
            List<String> batch = new ArrayList<>(ordered.subList(offset,
                    Math.min(offset + CLEANUP_DELETE_BATCH_SIZE, ordered.size())));
            long remainingMs = cleanupRemainingMs(deadline); // 배치마다 제한 시간을 초기화하지 않는다.
            System.out.printf("[Cleanup] phase=DELETE_REQUEST batch=%d/%d targets=%d total=%d timeoutRemainingMs=%d%n",
                    batchIndex, batchCount, batch.size(), ordered.size(), remainingMs);
            Map<String, org.apache.kafka.common.KafkaFuture<Void>> results = request.delete(batch, remainingMs);
            int alreadyAbsent = 0;
            for (String topic : batch) {
                org.apache.kafka.common.KafkaFuture<Void> result = results.get(topic);
                if (result == null) throw new IllegalStateException("Missing cleanup result for " + topic);
                try {
                    result.get(cleanupRemainingMs(deadline), TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    // 목록 조회 직후 이미 삭제된 토픽은 허용하되, 최종 목록 소멸 확인은 생략하지 않는다.
                    if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                        alreadyAbsent++;
                    } else {
                        throw e; // 권한/Controller/기타 오류는 성공으로 숨기지 않는다.
                    }
                }
            }
            System.out.printf("[Cleanup] phase=DELETE_ACK batch=%d/%d processed=%d/%d alreadyAbsent=%d%n",
                    batchIndex, batchCount, offset + batch.size(), ordered.size(), alreadyAbsent);
        }
    }

    // 전체 정리 제한 시간을 각 Kafka 요청과 대기에 공통으로 적용한다.
    private static long cleanupRemainingMs(long deadline) throws java.util.concurrent.TimeoutException {
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remaining <= 0) {
            throw new java.util.concurrent.TimeoutException("Experiment topic cleanup timed out");
        }
        return remaining;
    }
}
