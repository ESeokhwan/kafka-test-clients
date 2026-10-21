import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.TopicAuthorizationException;

// 실제 Kafka에 접속하지 않고 운영 cleanup의 분할 요청 함수를 직접 검증한다.
public class CleanupBatchTest {
    static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(10); }
    static Set<String> topics(int n) {
        Set<String> out = new HashSet<>();
        for (int i=0;i<n;i++) out.add("test_creator_0_"+i);
        return out;
    }
    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    static Map<String,KafkaFuture<Void>> success(List<String> batch) {
        Map<String,KafkaFuture<Void>> out = new HashMap<>();
        for (String name:batch) out.put(name,KafkaFuture.completedFuture(null));
        return out;
    }
    public static void main(String[] args) throws Exception {
        for (int n:new int[]{0,1,999,1000,1001,10000,10001,18000,216000}) {
            Set<String> original=topics(n), seen=new HashSet<>();
            AtomicInteger calls=new AtomicInteger();
            KafkaJmxProducerBatchDaemon.deleteCleanupBatches(original,deadline(),(batch,left)->{
                check(!batch.isEmpty() && batch.size()<=1000,"oversized batch");
                check(left>0,"deadline");
                for (String topic:batch) check(seen.add(topic),"duplicate topic");
                calls.incrementAndGet();
                return success(batch);
            });
            check(seen.equals(original),"missing or out-of-scope topics");
            check(calls.get()==(n+999)/1000,"batch count");
        }
        AtomicInteger calls=new AtomicInteger();
        KafkaJmxProducerBatchDaemon.deleteCleanupBatches(topics(1001),deadline(),(batch,left)->{
            Map<String,KafkaFuture<Void>> out=success(batch);
            KafkaFutureImpl<Void> absent=new KafkaFutureImpl<>();
            absent.completeExceptionally(new UnknownTopicOrPartitionException("already removed"));
            out.put(batch.get(0),absent);
            calls.incrementAndGet();
            return out;
        });
        check(calls.get()==2,"absent topic must not abort subsequent batch");
        try {
            KafkaJmxProducerBatchDaemon.deleteCleanupBatches(topics(1),deadline(),(batch,left)->{
                KafkaFutureImpl<Void> failure=new KafkaFutureImpl<>();
                failure.completeExceptionally(new TopicAuthorizationException("denied"));
                return Collections.singletonMap(batch.get(0),failure);
            });
            throw new AssertionError("authorization error swallowed");
        } catch (ExecutionException expected) {
            check(expected.getCause() instanceof TopicAuthorizationException,"wrong error");
        }
        try {
            KafkaJmxProducerBatchDaemon.deleteCleanupBatches(topics(1),System.nanoTime()-1,(batch,left)->{
                throw new AssertionError("request issued after deadline");
            });
            throw new AssertionError("deadline ignored");
        } catch (TimeoutException expected) { }
        try {
            KafkaJmxProducerBatchDaemon.deleteCleanupBatches(topics(1001),
                    System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(50),(batch,left)->{
                Map<String,KafkaFuture<Void>> out=success(batch);
                out.put(batch.get(0),new KafkaFutureImpl<>());
                return out;
            });
            throw new AssertionError("pending response exceeded deadline");
        } catch (TimeoutException expected) { }
        System.out.println("CleanupBatchTest PASS: boundaries, 216000 topics, absence race, authorization, deadlines");
    }
}
