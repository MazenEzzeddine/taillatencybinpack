
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPackLag {
    //TODO give fup and fdown as paramters to the functions.
    private static final Logger log = LogManager.getLogger(BinPackLag.class);

    static   Instant LastUpScaleDecision = Instant.now();
    //0.5 WSLA is reached around 85 events/sec
 static double wsla = 0.5;

    static double rebTime= 2.0;



    public static  void scaleAsPerBinPack() {

        log.info("Currently we have this number of consumers group {} {}","testgroup1", BinPackState.size );

        for (int i = 0; i < 5; i++) {
            ArrivalProducer.topicpartitions.get(i).setLag(ArrivalProducer.topicpartitions.get(i).getLag()
                    + (long) ((ArrivalProducer.totalArrivalrate * rebTime)/5.0));
        }

        if (BinPackState.action.equals("up")) {
            int neededsize = binPackAndScale();
            log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
            int replicasForscale = neededsize - BinPackState.size;
            if (replicasForscale > 0) {
                //TODO IF and Else IF can be in the same logic
                log.info("We have to upscale  group1 by {}", replicasForscale);
                // neededsize=5;
                BinPackState.size = neededsize;
                LastUpScaleDecision = Instant.now();
/*            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (final KubernetesClient k8s = new KubernetesClientBuilder().build()DefaultKubernetesClient()) {
                        k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
                        log.info("I have Upscaled group {} you should have {}", "testgroup1", neededsize);
                    }
                }
            }).start();*/

            try (final KubernetesClient k8s = new KubernetesClientBuilder().build() ) {
                k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
                log.info("I have Upscaled group {} you should have {}", "testgroup1", neededsize);
            }

            }
        } else if (BinPackState.action.equals("down")) {
            int neededsized = binPackAndScaled();
            int replicasForscaled =  BinPackState.size - neededsized;
            if (replicasForscaled <  BinPackState.size ) {
                log.info("We have to downscale  group by {} {}", "testgroup1", replicasForscaled);
                BinPackState.size = neededsized;
                LastUpScaleDecision = Instant.now();

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


                try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
                    k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsized);
                    log.info("I have downscaled group {} you should have {}", "testgroup1", neededsized);
                }


            }
        }
        log.info("===================================");
    }


    private  static int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalProducer.topicpartitions);

        float fraction = 0.9f;

        for (Partition partition : parts) {
            if (partition.getLag() > 175*wsla * fraction/*dynamicAverageMaxConsumptionRate*wsla*/) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), 175*wsla* fraction/*dynamicAverageMaxConsumptionRate*wsla*/);
                partition.setLag((long)(175*wsla* fraction/*dynamicAverageMaxConsumptionRate*wsla*/));
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > 175 *fraction/*dynamicAverageMaxConsumptionRate*wsla*/) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f",175f *fraction /*dynamicAverageMaxConsumptionRate*wsla*/));
                partition.setArrivalRate(175f*fraction /*dynamicAverageMaxConsumptionRate*wsla*/);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)),  (long)(175*wsla*fraction),
                        175*fraction/*dynamicAverageMaxConsumptionRate*wsla*/));
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
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }

    private static  int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalProducer.topicpartitions);
        double fractiondynamicAverageMaxConsumptionRate = 175*0.4;
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

        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }


}
