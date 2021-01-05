package flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MyFlink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("119.45.137.84", 8888);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] ss = s.split(" ");
                for (String s1 :
                        ss
                ) {
//                    HashMap hashMap = new HashMap();
//                    hashMap.put(s1, 1L);

                    collector.collect(new Tuple2(s1, 1L));
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);
        sum.print();
        executionEnvironment.execute("windowwordccountjava");
    }
}
