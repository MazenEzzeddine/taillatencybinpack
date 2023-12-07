import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);
    // static BinPack4 bp;
     static BinPackState bps;
    static BinPackLag bpl;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        initialize();
    }


    private static void initialize() throws InterruptedException, ExecutionException {
        bps = new BinPackState();
        bpl = new BinPackLag();


        Lag.readEnvAndCrateAdminClient();
        log.info("Warming 15  seconds.");
        Thread.sleep(15 * 1000);


        while (true) {
            log.info("Querying Prometheus");
            ArrivalProducer.callForArrivals();
            Lag.getCommittedLatestOffsetsAndLag();
            log.info("--------------------");
            log.info("--------------------");



            //scaleLogic();
            scaleLogicTail();



            log.info("Sleeping for 1 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(1000);
        }
    }



//    private static void scaleLogic() throws InterruptedException {
//
//        if  (Duration.between(bp.LastUpScaleDecision, Instant.now()).getSeconds() >10) {
//            bp.scaleAsPerBinPack();
//
//        } else {
//            log.info("No scale group 1 cooldown");
//        }
//
//
//    }



    private static void scaleLogicTail() throws InterruptedException {
        if  (Duration.between(BinPackLag.LastUpScaleDecision, Instant.now()).getSeconds() >10) {
            BinPackState.scaleAsPerBinPack();
            if (BinPackState.action.equals("up") || BinPackState.action.equals("down") ) {
                BinPackLag.scaleAsPerBinPack();
            }
        } else {
            log.info("No scale group 1 cooldown");
        }
    }





  /*  private static void scaleLogicReb() throws InterruptedException {

        if (Duration.between(bp.LastUpScaleDecision, Instant.now()).getSeconds() > 10) {
            boolean reply = bp.scaleAsPerBinPackPrologue();

            if (reply) {
                bp.scaleAsPerBinPack();
            }
        } else {
            log.info("No scale group 1 cooldown");
        }


    }
*/

}
