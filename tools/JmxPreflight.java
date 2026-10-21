import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/** 토픽 생성/삭제 없이 registry → RMI 연결 → Kafka MBean 조회를 검증한다. */
public final class JmxPreflight {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: JmxPreflight HOST PORT");
        String url = "service:jmx:rmi:///jndi/rmi://" + args[0] + ":" + args[1] + "/jmxrmi";
        // Shell에도 전체 제한 시간이 있다. RMI 응답 자체에도 제한을 적용한다.
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "15000");
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", "15000");
        System.setProperty("sun.rmi.transport.connectionTimeout", "15000");
        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url))) {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            Set<ObjectName> beans = connection.queryNames(new ObjectName("kafka.server:*"), null);
            if (beans.isEmpty()) throw new IllegalStateException("Kafka server MBeans not found");
            System.out.printf("[JMX Preflight] READY host=%s port=%s kafkaMBeans=%d%n",
                    args[0], args[1], beans.size());
        }
    }
}
