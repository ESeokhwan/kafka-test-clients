package org.example.apps;

import java.lang.reflect.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;

/** 브로커 접속 없이 실패·첫 메시지·성공 계측을 검증한다. */
public class CallbackDiagnosticsTest {
    private static Object field(Object obj, String name) throws Exception {
        Field f=obj.getClass().getDeclaredField(name); f.setAccessible(true); return f.get(obj);
    }
    private static void set(Object obj,String name,Object value) throws Exception {
        Field f=obj.getClass().getDeclaredField(name); f.setAccessible(true);f.set(obj,value);
    }
    private static void check(boolean condition,String message) {
        if(!condition) throw new AssertionError(message);
    }
    public static void main(String[] args) throws Exception {
        AutoTopicProducerMetrics app=new AutoTopicProducerMetrics();
        Properties p=new Properties();
        p.put("bootstrap.servers","localhost:9092");
        p.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        ProducerConfig config=new ProducerConfig(p);
        set(app,"callbackConfig",config);
        Class<?> m=Class.forName("org.example.apps.AutoTopicProducerMetrics$TopicMetrics");
        Constructor<?> mc=m.getDeclaredConstructor(String.class);mc.setAccessible(true);
        Object metrics=mc.newInstance("diagnostic_test_topic");
        Class<?> c=Class.forName("org.example.apps.AutoTopicProducerMetrics$TopicProducerCallback");
        Constructor<?> cc=c.getDeclaredConstructors()[0]; cc.setAccessible(true);
        for(int i=0;i<8;i++) {
            Callback cb=(Callback)cc.newInstance(app,i==0,System.currentTimeMillis(),System.currentTimeMillis(),metrics,i,System.nanoTime()-123_000_000L);
            cb.onCompletion(null,i==0?new TimeoutException("synthetic timeout"):new OutOfOrderSequenceException("synthetic sequence error"));
        }
        check(((AtomicInteger)field(metrics,"callbackFailureCount")).get()==8,"failure count");
        check(((AtomicInteger)field(metrics,"firstRecordFailureCount")).get()==1,"first failure count");
        check(field(metrics,"topicCreateLatencyMs")==null,"failed first must not become success");
        set(app,"ignoreResponse",true);
        ((Callback)cc.newInstance(app,false,0L,0L,metrics,8,System.nanoTime())).onCompletion(null,new TimeoutException("ignored-response failure"));
        check(((AtomicInteger)field(metrics,"callbackFailureCount")).get()==9,"ignoreResponse must not hide errors");
        set(app,"ignoreResponse",false);
        ((Callback)cc.newInstance(app,true,System.currentTimeMillis(),System.currentTimeMillis(),metrics,0,System.nanoTime())).onCompletion(null,null);
        check(field(metrics,"topicCreateLatencyMs")!=null,"successful first metric");
        ((Callback)cc.newInstance(app,false,System.currentTimeMillis(),System.currentTimeMillis(),metrics,1,System.nanoTime())).onCompletion(null,null);
        Method count=m.getDeclaredMethod("produceSuccessCount");count.setAccessible(true);
        check(((Long)count.invoke(metrics))==1L,"ordinary successful sample");
        check(config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)==120000,"effective delivery timeout");
        System.out.println("PASS CallbackDiagnosticsTest: failures, first-record accounting, ignored responses, success metrics and effective timeout");
    }
}
