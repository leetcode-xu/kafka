package kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        //kafka集群地址
        properties.put("bootstrap.servers", "119.45.137.84:9092,119.45.224.84:9092,119.45.210.252:9092");
        //消费者组id
        properties.put("group.id", "xu");
        //自动提交偏移量
        properties.put("enable.auto.commit", "true");
        //自动提交偏移量的时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        //默认是latest
        //earliest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none : topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        properties.put("auto.offset.reset","earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> objectObjectKafkaConsumer = new KafkaConsumer<>(properties);
//        objectObjectKafkaConsumer.subscribe(Arrays.asList("test"));
        TopicPartition part = new TopicPartition("test", 0);
        TopicPartition part2 = new TopicPartition("test", 1);
        TopicPartition part3 = new TopicPartition("test", 2);
        objectObjectKafkaConsumer.assign(Arrays.asList(part, part2, part3));
        while (true){
            ConsumerRecords<String, String> poll = objectObjectKafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> p: poll){
                int partition = p.partition();
                //该消息对应的key
                String key = p.key();
                //该消息对应的偏移量
                long offset = p.offset();
                //该消息内容本身
                String value = p.value();
                System.out.println("partition:"+partition+"\t key:"+key+"\toffset:"+offset+"\tvalue:"+value);
//                Thread.sleep(1000);
            }
        }
    }
}
