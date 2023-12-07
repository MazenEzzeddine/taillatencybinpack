import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class KafkaConsumerConfig {
    private static final Logger log = LogManager.getLogger(KafkaConsumerConfig.class);
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final String autoOffsetReset = "earliest";
    private final String enableAutoCommit = "false";



    public KafkaConsumerConfig(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;

    }
    public static KafkaConsumerConfig fromEnv() {
        String bootstrapServers = "my-cluster-kafka-bootsrap:9092"; //System.getenv("my-cluster-kafka-bootsrap:9092");
        String topic = "testtopic1";//System.getenv("testtopic11");
        String groupId = "testgroup1";//System.getenv("testgroup1");

        return new KafkaConsumerConfig(bootstrapServers, topic, groupId);
    }

    public static Properties createProperties(KafkaConsumerConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getEnableAutoCommit());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
       // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        // "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }
    public String getTopic() {
        return topic;
    }
    public String getGroupId() {
        return groupId;
    }


    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }
    public String getEnableAutoCommit() {
        return enableAutoCommit;
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
            "bootstrapServers='" + bootstrapServers + '\'' +
            ", topic='" + topic + '\'' +
            ", groupId='" + groupId + '\'' +
            ", autoOffsetReset='" + autoOffsetReset + '\'' +
            ", enableAutoCommit='" + enableAutoCommit + '\'' +

            '}';
    }
}
