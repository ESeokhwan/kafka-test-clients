package org.example.apps;

import java.io.PrintWriter;
import java.io.StringWriter;

/** 네트워크 없이 순서 역전, 실패, 그룹 경계, 히스토그램 및 재현성을 확인한다. */
public class ProducerTimingProbeTest {
    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    static String[] row(String csv, String group, String measure) {
        return csv.lines().filter(l -> l.startsWith("t," + group + "," + measure + ","))
                .findFirst().orElseThrow().split(",", -1);
    }
    public static void main(String[] args) throws Exception {
        var p = new ProducerTimingProbe("t", 0, 10_000_000, 123_000, 456_000, 789_000);
        var first = p.begin(0, 0, 1_000_000, 1_050_000);
        first.returned(1_250_000);
        first.completed(1_550_000, false);
        first.completed(9_000_000, false); // 중복 통지 방어
        var early = p.begin(1, 999_999_999, 2_000_000, 2_000_000);
        early.completed(2_300_000, false);
        early.returned(2_500_000); // send가 반환되기 전에 ACK
        var failure = p.begin(2, 1_000_000_000, 3_000_000, 3_000_000);
        failure.completed(203_000_001, true);
        failure.returned(203_000_001);
        // 두 스레드가 동시에 완료하는 경우에도 한 번만 합산한다.
        for (int i = 0; i < 100; i++) {
            var s = p.begin(i + 3, 2_000_000_000L, 0, 10_000_000);
            Thread a = new Thread(() -> s.returned(10_100_000));
            Thread b = new Thread(() -> s.completed(10_200_000, false));
            a.start(); b.start(); a.join(); b.join();
        }
        p.resetSchedule();
        var buffer = new StringWriter();
        var out = new PrintWriter(buffer);
        ProducerTimingProbe.header(out);
        p.write(out); out.flush();
        String csv = buffer.toString();
        check(row(csv,"first","send_call")[4].equals("200.000"),"send duration");
        check(row(csv,"first","send_to_ack")[4].equals("500.000"),"ack duration");
        check(row(csv,"first","post_return_to_ack")[4].equals("300.000"),"post-return duration");
        check(row(csv,"after_first_under_1s","post_return_to_ack")[4].equals("0.000"),"early ack clamp");
        check(row(csv,"after_first_under_1s","send_call")[12].equals("1"),"early ack count");
        check(row(csv,"from_1s","send_to_ack")[3].equals("100"),"failed ack excluded");
        check(row(csv,"from_1s","send_call")[3].equals("101"),"all completed sends");
        check(row(csv,"from_1s","send_call")[8].equals("1"),"overflow count");
        check(row(csv,"from_1s","send_call")[11].equals("1"),"failure count");
        check(row(csv,"first","send_call")[5].equals("200"),"percentile upper bound");
        check(row(csv,"first","schedule_lag")[17].split(";").length==100,"phase bins");
        for (int i=0; i<1000; i++) {
            long delay=ProducerTimingProbe.phaseDelay(42,"topic"+i,10_000_000);
            check(delay>=0 && delay<10_000_000,"jitter range");
            check(delay==ProducerTimingProbe.phaseDelay(42,"topic"+i,10_000_000),"reproducibility");
        }
        check(ProducerTimingProbe.phaseDelay(42,"topic",0)==0,"default unchanged");
        System.out.println("PASS ProducerTimingProbeTest");
    }
}
