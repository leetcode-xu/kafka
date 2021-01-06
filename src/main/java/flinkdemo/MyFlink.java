package flinkdemo;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.*;

public class MyFlink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("119.45.137.84", 8888);
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
                String[] ss = s.split(" ");
                for (String s1 :
                        ss
                ) {
//                    HashMap hashMap = new HashMap();
//                    hashMap.put(s1, 1L);

                    collector.collect(new Tuple3(s1, 1L, System.currentTimeMillis()));
                }
            }
        })
                .keyBy(0)
//                .timeWindow(Time.seconds(2))
//                .windowAll()
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(20)))
//                .timeWindowAll(Time.seconds(2))
                .sum(1);
        sum.print();

        executionEnvironment.execute("windowwordccountjava");
    }
}
