
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MultiwayStreamGenerator {
    private long period;
    private int threadsNum;
    private DSTuple tuple;


    MultiwayStreamGenerator(long period, int threadsNum, DSTuple tuple){
//        Part I.       Set period & threads' amount
        this.period = period;
        this.threadsNum = threadsNum;
//        Part II.      Set tuple property
        this.tuple = tuple;
    }

    public void start(){
//        Calculate choose-rate
        tuple.calChooseRate();
//        System.out.println("Enter ok to start generation");
//        try {
//            System.in.read();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        Set generation task
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                tuple.produceTuple();
            }
        };

//        Create a thread_pool
        ScheduledExecutorService pump = Executors.newScheduledThreadPool(threadsNum);
        pump.scheduleAtFixedRate(runnable,0, period, TimeUnit.MILLISECONDS);
    }

}


