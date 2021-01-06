package flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

class Richff extends RichFlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
    private transient ValueState<Tuple2<Long, Double>> state;
    @Override
    public void flatMap(Tuple2<Long, Double> longLongTuple2, Collector<Tuple2<Long, Double>> collector) throws Exception {
            Tuple2<Long, Double> start_state = state.value();
            state.update(Tuple2.of(start_state.f0+longLongTuple2.f0, start_state.f1+longLongTuple2.f1));
            if (state.value().f0>=2){
            collector.collect(Tuple2.of(longLongTuple2.f0, state.value().f1 / state.value().f0));
//            state.clear();
            }
    }
    @Override
    public void open(Configuration config) throws IOException {
        ValueStateDescriptor<Tuple2<Long, Double>> descriptor =
        new ValueStateDescriptor<Tuple2<Long, Double>>(
        "average", // the state name
        TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {}), // type information
        Tuple2.of(0L, 0d)); // default value of the state, if nothing was set
        state = getRuntimeContext().getState(descriptor);
//        System.out.println(state.value().f0);
    }
}

public class AverageFlink {
    public static void main(String[] args) throws Exception, IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Double>> datastream = env.fromElements(new Tuple2<Long, Double>(1L, 3d), new Tuple2<Long, Double>(1L, 2d), new Tuple2<Long, Double>(1L, 5d));
        SingleOutputStreamOperator<Tuple2<Long, Double>> tuple2SingleOutputStreamOperator = datastream.keyBy(0).flatMap(new Richff());
        tuple2SingleOutputStreamOperator.print();
        env.execute("aveageflinkdemo");


    }
}
