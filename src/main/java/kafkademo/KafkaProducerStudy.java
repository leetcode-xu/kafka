package kafkademo;

import akka.event.Logging;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

class MyProducerRecord extends ProducerRecord<String, String>{
    public String topic;
    public String value;
    public MyProducerRecord(String topic, String key, String value) {
        super(topic, key, value);
        this.value = value;


    }
}


class MyCallBack implements Callback{

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println(recordMetadata.toString());
        System.out.println(e.toString());
    }
}
public class KafkaProducerStudy {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        Logging.DebugLevel();
        Logging.ErrorLevel();
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "119.45.210.252:9092");
        properties.put("bootstrap.servers", "119.45.137.84:9092,119.45.224.84:9092,119.45.210.252:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("partitioner.class","kafkademo.MyPartitioner");
        properties.put("request.timeout.ms", 60000);
        //缓冲区的大小  //默认32M
        properties.put("buffer.memory", 33554432);
        //批处理数据的大小，每次写入多少数据到topic   //默认16KB
//        properties.put("batch.size", 16384);
        //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);
//        RecordMetadata recordMetadata = producer.send(new MyProducerRecord("test", "3", "sdfdsf")).get();
        int s = 0;
        for(;s<100;s++){
            Future<RecordMetadata> send = producer.send(new MyProducerRecord("test", "" + s, "sdfdsf"+s));
            Thread.sleep(1000*1);
        }


        producer.close();

//        System.out.println(recordMetadata.offset());
    }
}
