package kafkademo;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //获取topic分区数
        int partitions = cluster.partitionsForTopic(topic).size();

        //key.hashCode()可能会出现负数 -1 -2 0 1 2
        //Math.abs 取绝对值
        System.out.println("key: "+key);
        System.out.println("o1: "+o1);
        System.out.println("bytes: "+ bytes);
        System.out.println("bytes1: "+ bytes1);
        System.out.println("cluster: "+ cluster.partitionsForTopic("test"));

        return Math.abs(key.hashCode()% partitions);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}