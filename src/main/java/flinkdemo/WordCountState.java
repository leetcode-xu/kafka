package flinkdemo;

import akka.dispatch.Foreach;
import org.apache.commons.math3.analysis.function.StepFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.xml.dtd.impl.WordExp;

import java.lang.reflect.Type;


class MyFlatMapFunction extends RichFlatMapFunction<Tuple2<String, Long>,Tuple2<String, Long>> {

    private ValueState<Tuple2<String, Long>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<String, Long>> count = new ValueStateDescriptor<>("count", Types.TUPLE(Types.STRING, Types.LONG));
        state = getRuntimeContext().getState(count);
    }


    @Override
    public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, Long>> collector) throws Exception {
        Tuple2<String, Long> value = state.value();
        if(null==value){
            state.update(Tuple2.of(stringLongTuple2.f0, 1L));
            value = Tuple2.of(stringLongTuple2.f0, 1L);
        }
        else{
            value = Tuple2.of(stringLongTuple2.f0, value.f1 + 1L);
            state.update(Tuple2.of(stringLongTuple2.f0, value.f1 + 1L));
        }
        System.out.println(state.value().f0);
        collector.collect(Tuple2.of(stringLongTuple2.f0, value.f1));
    }
}

public class WordCountState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("119.45.137.84", 8888);
        socket.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String word:
                        s1) {
                    collector.collect(Tuple2.of(word, 1L));

                }
            }
        })
                .keyBy(0)
                .flatMap(new MyFlatMapFunction())
                .print();
        JobExecutionResult wordcount = env.execute("wordcount");
    }
}
