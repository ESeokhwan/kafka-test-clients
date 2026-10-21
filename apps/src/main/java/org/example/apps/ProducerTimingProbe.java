package org.example.apps;

import java.io.PrintWriter;
import java.util.Locale;
import java.util.SplittableRandom;

/**
 * 진단 전용 집계기. 매 메시지 파일 쓰기 없이 고정 크기 히스토그램을 사용한다.
 * 기존 metrics CSV의 모집단이나 전송 방식을 변경하지 않는다.
 */
final class ProducerTimingProbe {
    private final String topic;
    private final long epoch;
    private final long interval;
    private final long workerOffset;
    private final long initNanos;
    private final long phaseDelay;
    private final long[][] phases = new long[3][100];
    private final Stats[][] stats = new Stats[3][4];
    private final long[] attempted = new long[3], failed = new long[3], earlyAck = new long[3];
    private final long[] completed = new long[3];
    private long scheduleResets;
    private long productionStart;

    ProducerTimingProbe(String topic, long epoch, long interval, long workerOffset,
                        long initNanos, long phaseDelay) {
        this.topic = topic;
        this.epoch = epoch;
        this.interval = interval;
        this.workerOffset = workerOffset;
        this.initNanos = initNanos;
        this.phaseDelay = phaseDelay;
        for (int g = 0; g < 3; g++)
            for (int m = 0; m < 4; m++) stats[g][m] = new Stats();
    }

    // 같은 seed와 토픽 이름이면 같은 지연을 선택한다. 상한은 포함하지 않는다.
    static long phaseDelay(long seed, String topic, long boundNanos) {
        return boundNanos == 0 ? 0 : new SplittableRandom(seed ^ topic.hashCode()).nextLong(boundNanos);
    }

    synchronized Sample begin(int index, long ageNanos, long scheduled, long enter) {
        // 첫 메시지는 PRECREATE/AUTO 모두 별도 분류한다. 나머지는 send 진입 시점의 생애 나이 기준이다.
        int group = index == 0 ? 0 : ageNanos < 1_000_000_000L ? 1 : 2;
        attempted[group]++;
        stats[group][0].add(Math.max(0, enter - scheduled));
        int bin = (int) ((Math.floorMod(enter - epoch, interval) / (double) interval) * 100);
        phases[group][Math.min(99, bin)]++;
        return new Sample(this, group, enter);
    }

    synchronized void resetSchedule() { scheduleResets++; }
    synchronized void productionStarted(long time) { productionStart = time - epoch; }

    private synchronized void finish(int group, long enter, long returned, long ack, boolean error) {
        completed[group]++;
        stats[group][1].add(Math.max(0, returned - enter));
        if (error) {
            failed[group]++;
            return; // 실패를 성공 ACK latency에 섞지 않는다.
        }
        stats[group][2].add(Math.max(0, ack - enter));
        // Kafka 콜백은 send가 반환되기 전에 실행될 수도 있다.
        if (ack < returned) earlyAck[group]++;
        stats[group][3].add(Math.max(0, ack - returned));
    }

    static void header(PrintWriter out) {
        out.println("Topic,Group,Measure,Count,MeanUs,P90UpperUs,P99UpperUs,MaxUs,OverflowAbove100ms,"
                + "Attempted,Completed,Failed,AckBeforeReturn,ScheduleResets,WorkerStartOffsetUs,"
                + "ProducerInitUs,PhaseDelayUs,PhaseBinCounts,ProductionStartOffsetUs,WorkerEndOffsetUs");
    }

    synchronized void write(PrintWriter out) {
        long workerEnd = System.nanoTime() - epoch;
        String[] groups = {"first", "after_first_under_1s", "from_1s"};
        String[] measures = {"schedule_lag", "send_call", "send_to_ack", "post_return_to_ack"};
        for (int g = 0; g < 3; g++) {
            StringBuilder phase = new StringBuilder();
            for (long n : phases[g]) {
                if (phase.length() > 0) phase.append(';');
                phase.append(n);
            }
            for (int m = 0; m < 4; m++) {
                Stats s = stats[g][m];
                out.printf(Locale.ROOT, "%s,%s,%s,%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%s,%.3f,%.3f%n",
                        topic, groups[g], measures[m], s.count, s.mean(), s.percentile(.90),
                        s.percentile(.99), s.count == 0 ? "" : String.format(Locale.ROOT, "%.3f", s.max / 1000.0),
                        s.bins[1001], attempted[g], completed[g], failed[g], earlyAck[g], scheduleResets,
                        workerOffset / 1000.0, initNanos / 1000.0, phaseDelay / 1000.0,
                        m == 0 ? phase : "", productionStart / 1000.0, workerEnd / 1000.0);
            }
        }
    }

    // send 반환과 callback의 실행 순서에 의존하지 않고 두 시각이 확보되면 정확히 한 번 집계한다.
    static final class Sample {
        private final ProducerTimingProbe owner;
        private final int group;
        private final long enter;
        private long returned, ack;
        private boolean hasReturn, hasAck, done, error;
        Sample(ProducerTimingProbe owner, int group, long enter) {
            this.owner = owner; this.group = group; this.enter = enter;
        }
        synchronized void returned(long time) {
            if (hasReturn) return;
            returned = time; hasReturn = true; commit();
        }
        synchronized void completed(long time, boolean failed) {
            if (hasAck) return;
            ack = time; error = failed; hasAck = true; commit();
        }
        private void commit() {
            if (!done && hasReturn && hasAck) {
                done = true;
                owner.finish(group, enter, returned, ack, error);
            }
        }
    }

    private static final class Stats {
        // 0은 정확한 0, 1..1000은 100us 폭의 상한 버킷, 마지막은 >100ms.
        private final long[] bins = new long[1002];
        private long count, max;
        private double total;
        void add(long nanos) {
            count++; total += nanos; max = Math.max(max, nanos);
            bins[nanos > 100_000_000L ? 1001 : (int) ((nanos + 99_999L) / 100_000L)]++;
        }
        String mean() {
            return count == 0 ? "" : String.format(Locale.ROOT, "%.3f", total / count / 1000.0);
        }
        String percentile(double q) {
            if (count == 0) return "";
            long target = (long) Math.ceil(q * count), sum = 0;
            for (int i = 0; i < bins.length; i++) {
                sum += bins[i];
                if (sum >= target) return i == 1001 ? ">100000" : Integer.toString(i * 100);
            }
            throw new IllegalStateException("Histogram count mismatch");
        }
    }
}
