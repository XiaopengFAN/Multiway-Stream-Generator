
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MultiwayStreamGenerator {
    private long period;
    private int threadsAmount;
    private DSTuple tuple;
    private Properties kafkaProps = new Properties();
    private KafkaProducer producer;
    private String topic;

    MultiwayStreamGenerator(long period, int threadsAmount, DSTuple tuple, String topic){
//        Part I.       Set period & threads' amount
        this.period = period;
        this.threadsAmount = threadsAmount;
//        Part II.      Set tuple property
        this.tuple = tuple;
    }

    MultiwayStreamGenerator(long period, int threadsAmount, DSTuple tuple, String topic,String servers){
//        Part I.       Set period & threads' amount
        this.period = period;
        this.threadsAmount = threadsAmount;
//        Part II.      Set tuple property
        this.tuple = tuple;
//        Part III.     Set Kafka conf
        this.topic = topic;
        kafkaProps.put("bootstrap.servers",servers);
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    public void start(){
//        Set generation task
        Runnable runnable = new Runnable() {
            public void run() {
                String text = tuple.produceTuple();
                producer.send(new ProducerRecord<String,String>(String.valueOf(new Date().getTime()),text));
//                System.out.println(text);
            }
        };

//        Create a thread_pool
        ScheduledExecutorService pump = Executors.newScheduledThreadPool(threadsAmount);
        for (int i = 0; i < threadsAmount; i++) {
            pump.scheduleAtFixedRate(runnable,0, period, TimeUnit.MILLISECONDS);
        }
    }

}


