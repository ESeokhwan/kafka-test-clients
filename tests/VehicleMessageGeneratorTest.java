package org.example.apps;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;

/** 브로커 연결 없이 payload 크기·식별자·패딩 재사용·차량 간 격리를 검증한다. */
public class VehicleMessageGeneratorTest {
    private static void check(boolean value, String detail) {
        if (!value) throw new AssertionError(detail);
    }

    public static void main(String[] args) throws Exception {
        VehicleMessageGenerator generator = new VehicleMessageGenerator(4096);
        String first = generator.generate("test_creator_0_1_0");
        String pad = first.substring(first.indexOf('!') + 1);
        for (int i : new int[] {1, 9, 10, 99, 100, 999, 1000, 3999, Integer.MAX_VALUE}) {
            String id = "test_creator_0_1_" + i;
            String value = generator.generate(id);
            check(value.startsWith(id + "!"), "message ID changed");
            check(value.getBytes(StandardCharsets.UTF_8).length == 4096, "UTF-8 size changed");
            check(value.substring(id.length() + 1).equals(pad.substring(0, 4096-id.length()-1)), "padding regenerated");
        }
        check(first.equals(generator.generate("test_creator_0_1_0")), "previous async payload changed");
        check(generator.generate("차량_1").getBytes(StandardCharsets.UTF_8).length == 4096, "Unicode byte size");
        check(new VehicleMessageGenerator(1).generate("").equals("!"), "minimum size");
        check(new VehicleMessageGenerator(4).generate("abc").equals("abc!"), "exact fit");
        try {
            new VehicleMessageGenerator(4).generate("abcd");
            throw new AssertionError("oversize ID accepted");
        } catch (IllegalArgumentException expected) { }
        try {
            new VehicleMessageGenerator(0);
            throw new AssertionError("zero size accepted");
        } catch (IllegalArgumentException expected) { }
        var pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int car = 0; car < 200; car++) {
                final int vehicle = car;
                tasks.add(() -> {
                    VehicleMessageGenerator own = new VehicleMessageGenerator(4096);
                    String retained = own.generate("vehicle_" + vehicle + "_0");
                    for (int i = 0; i < 1000; i++) {
                        String id = "vehicle_" + vehicle + "_" + i;
                        String value = own.generate(id);
                        check(value.length() == 4096 && value.startsWith(id + "!"), "vehicle isolation");
                    }
                    return retained.equals(own.generate("vehicle_" + vehicle + "_0"));
                });
            }
            for (var result : pool.invokeAll(tasks)) check(result.get(), "retained payload mutated");
        } finally {
            pool.shutdownNow();
        }
        System.out.println("PASS: fixed bytes, sequence boundaries, cached padding, async immutability, 200 vehicle generators");
    }
}
