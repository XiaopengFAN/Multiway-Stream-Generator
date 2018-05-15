
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MultiwayStreamGenerator {
    private long period;
    private int threadsAmount;
    private DSChoosemethod[] chmethods;
    private DSTuple tuple;


    MultiwayStreamGenerator(long period, int threadsAmount, DSTuple tuple){
//        Part I.       Set period & threads' amount
        this.period = period;
        this.threadsAmount = threadsAmount;
//        Part II.      Set tuple property
        this.tuple = tuple;
    }

    public void start(){
//        Set generation task
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                tuple.produceTuple();
            }
        };

//        Create a thread_pool
        ScheduledExecutorService pump = Executors.newScheduledThreadPool(threadsAmount);
        pump.scheduleAtFixedRate(runnable,0, period, TimeUnit.MILLISECONDS);
    }

}


