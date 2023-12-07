
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class BinPackState2 {
    //TODO give fup and fdown as paramters to the functions.
    private static final Logger log = LogManager.getLogger(BinPackState2.class);
    public static   int size =1;
    public   Instant LastUpScaleDecision = Instant.now();
    //0.5 WSLA is reached around 85 events/sec
    static double wsla = 0.5;

    static String action = "none";


    static List<Consumer> assignment = new ArrayList<Consumer>();
    static List<Consumer> currentAssignment = assignment;

    private static KafkaConsumer<byte[], byte[]> metadataConsumer;

    public  static void scaleAsPerBinPack() {
        action = "none";
        log.info("Currently we have this number of consumers group {} {}","testgroup1", size );
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - size;
        if (replicasForscale > 0) {
            action = "up";
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale  group1 by {}", replicasForscale);
           // neededsize=5;
            //size = neededsize;
/*            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (final KubernetesClient k8s = new KubernetesClientBuilder().build()DefaultKubernetesClient()) {
                        k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
                        log.info("I have Upscaled group {} you should have {}", "testgroup1", neededsize);
                    }
                }
            }).start();*/

//            try (final KubernetesClient k8s = new KubernetesClientBuilder().build() ) {
//                k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
//                log.info("I have Upscaled group {} you should have {}", "testgroup1", neededsize);
//            }


            currentAssignment = assignment;

            return;

        } else {
            int neededsized = binPackAndScaled();
            int replicasForscaled = size - neededsized;
            if (replicasForscaled > 0) {
               action = "down";
                log.info("We have to downscale  group by {} {}", "testgroup1", replicasForscaled);

                currentAssignment = assignment;

                return;


                //size = neededsized;

/*
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try (final KubernetesClient k8s = new KubernetesClientBuilder().build() *//*DefaultKubernetesClient()*//*) {
                            k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsized);
                            log.info("I have downscaled group {} you should have {}", "testgroup1", neededsized);
                        }

                    }
                }).start();*/


//                try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
//                    k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsized);
//                    log.info("I have downscaled group {} you should have {}", "testgroup1", neededsized);
//                }


            }
        }


        if (assignmentViolatesTheSLA()) {
            if (metadataConsumer == null) {
                KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
                Properties props = KafkaConsumerConfig.createProperties(config);
                metadataConsumer = new KafkaConsumer<>(props);
            }
            currentAssignment = assignment;
           // metadataConsumer.enforceRebalance();

            action = "REASS";
        }
        log.info("===================================");
    }


    private static   int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalProducer.topicpartitions);

        float fraction = 0.9f;

        for (Partition partition : parts) {
            if (partition.getLag() > 200*wsla * fraction/*dynamicAverageMaxConsumptionRate*wsla*/) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), 200*wsla* fraction/*dynamicAverageMaxConsumptionRate*wsla*/);
                partition.setLag((long)(200*wsla* fraction/*dynamicAverageMaxConsumptionRate*wsla*/));
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > 200 *fraction/*dynamicAverageMaxConsumptionRate*wsla*/) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f",200f *fraction /*dynamicAverageMaxConsumptionRate*wsla*/));
                partition.setArrivalRate(200f*fraction /*dynamicAverageMaxConsumptionRate*wsla*/);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)),  (long)(200*wsla*fraction),
                        200*fraction/*dynamicAverageMaxConsumptionRate*wsla*/));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {
                    if (consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }


         assignment = consumers;
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }

    static  int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalProducer.topicpartitions);
        double fractiondynamicAverageMaxConsumptionRate = 200*0.4;
        for (Partition partition : parts) {
            if (partition.getLag() > fractiondynamicAverageMaxConsumptionRate*wsla) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                                " we are truncating its lag", partition.getId(), partition.getLag(),
                        fractiondynamicAverageMaxConsumptionRate*wsla);
                partition.setLag((long)(fractiondynamicAverageMaxConsumptionRate *wsla));
            }
        }

        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > fractiondynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", fractiondynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(fractiondynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(consumerCount)),
                        (long)(fractiondynamicAverageMaxConsumptionRate*wsla),
                        fractiondynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers, Collections.reverseOrder());
                for (i = 0; i < consumerCount; i++) {

                    if (consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }


        assignment = consumers;


        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }


    private static  boolean assignmentViolatesTheSLA() {
        for (Consumer cons : currentAssignment) {
            if (cons.getRemainingLagCapacity() <  (long) (wsla*200*.9f)||
                    cons.getRemainingArrivalCapacity() < 200f*0.9f){
                return true;
            }
        }
        return false;
    }


}
