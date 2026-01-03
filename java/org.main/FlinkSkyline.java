package org.main;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.*;

public class FlinkSkyline {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = params.getInt("parallelism", 4);
        String algo = params.get("algo", "angle");
        String inputTopic = params.get("input-topic", "input-tuples");
        String queryTopic = params.get("query-topic", "queries");
        String outputTopic = params.get("output-topic", "output-skyline");
        double domainMax = params.getDouble("domain", 1000.0);

        env.setParallelism(parallelism);

        KafkaSource<String> tupleSrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> querySrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(queryTopic)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<ServiceTuple> tuples = env.fromSource(tupleSrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Tuples")
                .flatMap((String s, Collector<ServiceTuple> out) -> {
                    try {
                        String[] p = s.split(",");
                        double[] vals = new double[p.length - 1];
                        for(int i = 1; i < p.length; i++) vals[i-1] = Double.parseDouble(p[i]);
                        out.collect(new ServiceTuple(p[0], vals));
                    } catch (Exception ignored) {}
                }).returns(ServiceTuple.class);

        MapStateDescriptor<String, String> desc = new MapStateDescriptor<>("q", String.class, String.class);
        BroadcastStream<String> bQueries = env.fromSource(querySrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Queries")
                .broadcast(desc);

        DataStream<ServiceTuple> partitioned = tuples.keyBy(t -> {
            switch (algo.toLowerCase()) {
                case "mr-dim":
                    return (int) (t.values[0] / (domainMax / parallelism));
                case "mr-grid":
                    return (int) (t.values[0] / (domainMax/2)) + (int) (t.values[1] / (domainMax/2)) * 2;
                case "mr-angle":
                default:
                    double angle = Math.atan2(t.values[1], t.values[0]);
                    return (int) (angle / (Math.PI / 2 / parallelism));
            }
        });

        partitioned.connect(bQueries)
                .process(new SkylineProcessor())
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema()).build())
                        .build());

        env.execute("Distributed Skyline - " + algo);
    }
    //LOCAL skyline computation
    //we create the custom class SkylineProcessor based on the interface KeyedBroadcastProcessFunction
    //(this is expected when using .process() above.)
    //we use this implementation in order create a stateful operator since we are handling 2 input streams (tuples and queries)
    //(note to sunadelfos: using lambda functions could be messier or ronaldo-ier)
    public static class SkylineProcessor extends KeyedBroadcastProcessFunction<Integer, ServiceTuple, String, String> {
        private transient ListState<ServiceTuple> skyState;
        //creating functions according to the interface.
        @Override
        public void open(Configuration parameters) {
            skyState = getRuntimeContext().getListState(new ListStateDescriptor<>("sky", ServiceTuple.class));
        }

        //BNL part here
        @Override
        public void processElement(ServiceTuple value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            List<ServiceTuple> current = new ArrayList<>();
            boolean isDominated = false;
            //BNL is done here exactly, compare each tuple with every other
            for (ServiceTuple s : skyState.get()) {
                if (s.dominates(value))
                {
                    isDominated = true;
                    break;
                }
                if (!value.dominates(s)){
                    current.add(s);
                }
            }
            if (!isDominated) {
                current.add(value);
                skyState.update(current);
            }
        }

        @Override
        public void processBroadcastElement(String query, Context ctx, Collector<String> out) throws Exception {
            //safely accesses the keyed state from a broadcast context
            //we print the local skyline for each partition
            ctx.applyToKeyedState(new ListStateDescriptor<>("sky", ServiceTuple.class),
                    (Integer key, ListState<ServiceTuple> state) -> {
                        for (ServiceTuple s : state.get()) {
                            out.collect("QID " + query + " [" + key + "]: " + s.toString());
                        }
                    });
        }
    }
}
