import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Lag {
    private static final Logger log = LogManager.getLogger(Lag.class);
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static String topic;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static long totalLag;
    //////////////////////////////////////////////////////////////////////////////
    static ArrayList<Partition> partitions = new ArrayList<>();

    public  static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        topic = "testtopic1";
        CONSUMER_GROUP = "testgroup1";
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        for (int i = 0; i < 5; i++) {
            //ArrivalProducer.topicpartitions.get(i).setLag(0L);
            Partition p = new Partition(i,0L,0.0);
            partitions.add(p);
        }
    }


    public static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            requestLatestOffsets.put(new TopicPartition(topic, i), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
         totalLag=0L;
        for (int i = 0; i < 5; i++) {
            TopicPartition t = new TopicPartition(topic, i);
            long latestOffset = latestOffsets.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();
            partitions.get(i).setLag(latestOffset - committedoffset);
            ArrivalProducer.topicpartitions.get(i).setLag(latestOffset-committedoffset);
            totalLag += partitions.get(i).getLag();
            log.info("partition {} has lag {}", i, partitions.get(i).getLag());
        }
        //addParentLag(totalLag);
        log.info("total lag {}", totalLag);
    }


   /*static  void addParentLag (long totalLag) {

        double childarrivalrate = totalLag/0.5;
        log.info("after adding parent lag : Arrival rate into testtopic2");
        double totalArrivalRate = childarrivalrate + ArrivalProducer.totalArrivalrate;
       for (int i = 0; i < 5; i++) {
           ArrivalProducer.topicpartitions2.get(i).setArrivalRate(totalArrivalRate/5.0);

       }

    }*/
}
