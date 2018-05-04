
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UnboundedDataSource {
    private DSModel model;
    private long period;
    private int threadsNum;
    private DSTuple tuple;
    private DSChoosemethod chmethd;


    UnboundedDataSource(long period, int threadsNum, DSTuple tuple,
                            DSChoosemethod chmethd){
//        Part I.       Set period & threads' amount
        this.period = period;
        this.threadsNum = threadsNum;
//        Part II.      Set tuple property
        this.tuple = tuple;
//        Part III.     Set choosing-rate algorithm
        this.chmethd = chmethd;
    }

    public void start(){
//        Set task
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


