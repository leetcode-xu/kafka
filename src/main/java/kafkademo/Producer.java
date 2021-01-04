package kafkademo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.nsc.transform.patmat.ScalaLogic;


import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private final static Logger log = LoggerFactory.getLogger(Producer.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean isAsync;
    int messageNo = 1;

    public Producer(String topic, Properties props, boolean isAsync){
        this.producer = new KafkaProducer<String, String> (props);
        this.topic = topic;
        this.isAsync = isAsync;
    }
    public void close(){
        producer.close();
    }
    public void send(String key, String value) {
        long startTime = System.currentTimeMillis();
        log.info("*****" + key);

        //isAsync , kafka发送数据的时候，有两种方式
        //1: 异步发送
        //2: 同步发送
        //isAsync: true的时候是异步发送，false就是同步发送
        if (isAsync) { // Send asynchronously
            //异步发送
            //这样的方式，性能比较好，我们生产代码用的就是这种方式。
            producer.send(new ProducerRecord<>(topic,
                    key,
                    value), new KafkaSendCallBack(startTime, key, value, messageNo));
            log.debug("Sent message {}>topic:{} ({}, {}), ", messageNo, topic, key, value);
        } else { // Send synchronously
//            try {
                //同步发送
                //发送一条消息，等这条消息所有的后续工作都完成以后才继续下一条消息的发送。
                producer.send(new ProducerRecord<>(topic,
                        key,
                        value), new KafkaSendCallBack(startTime, key, value, messageNo));
//                producer.close();
                log.debug("Sent message {}>topic:{} ({}, {}), ", messageNo, topic, key, value);
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
        }
        ++messageNo;
    }



    private static class KafkaSendCallBack implements Callback {

        private final long startTime;
        private final String key;
        private final String message;
        private final long no;

        public KafkaSendCallBack(long startTime, String key, String message, long no) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
            this.no = no;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if(exception != null){
                log.error("Send Error!, Caught Exception: {}\tType: {}\tReason: {}", exception, exception.getClass().getName(), exception.getMessage());
                //一般我们生产里面 还会有其它的备用的链路。
            }else{
                log.info("Send success!, {}, sendTime:{}", key, this.startTime);
            }
            if (metadata != null) {
                log.info(
                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                                "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            }
        }
    }
}