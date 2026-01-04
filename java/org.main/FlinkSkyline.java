package org.main;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import java.io.Serializable;
import java.util.*;

public class FlinkSkyline {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        //parameters for the flink job
        final int parallelism = params.getInt("parallelism", 4);
        final String algo = params.get("algo", "mr-angle").toLowerCase();
        final String inputTopic = params.get("input-topic", "input-tuples");
        final String queryTopic = params.get("query-topic", "queries");
        final String outputTopic = params.get("output-topic", "output-skyline");
        final double domainMax = params.getDouble("domain", 1000.0);
        final int dims = params.getInt("dims", 2);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        //kafka topics for the tuples and the queries
        KafkaSource<String> tupleSrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> querySrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(queryTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //perform partition on data based on the algorithm given
        PartitioningLogic.SkylinePartitioner partitioner;
        switch (algo) {
            case "mr-dim": partitioner = new PartitioningLogic.DimPartitioner(parallelism, domainMax); break;
            case "mr-grid": partitioner = new PartitioningLogic.GridPartitioner(parallelism, domainMax, dims); break;
            default: partitioner = new PartitioningLogic.AnglePartitioner(parallelism);
        }

        //converting stream to type ServiceTuple
        KeyedStream<ServiceTuple, Integer> keyedData = env.fromSource(tupleSrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Data")
                .map(ServiceTuple::fromString)
                .filter(Objects::nonNull)
                .keyBy(partitioner);

        //trigger stream
        KeyedStream<Tuple2<Integer, String>, Integer> keyedTriggers = env.fromSource(querySrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Queries")
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public void flatMap(String queryId, Collector<Tuple2<Integer, String>> out) {
                        for (int i = 0; i < parallelism; i++) {
                            out.collect(new Tuple2<>(i, queryId));
                        }
                    }
                })
                .keyBy(t -> t.f0);

        //connect and compute Local Skyline
        DataStream<Tuple2<String, List<ServiceTuple>>> localSkylines = keyedData
                .connect(keyedTriggers)
                .process(new SkylineLocalProcessor())
                .name("LocalSkylineProcessor");

        //global aggregation
        DataStream<String> finalResults = localSkylines
                .keyBy(t -> t.f0)
                .process(new GlobalSkylineAggregator(parallelism))
                .name("GlobalReducer");

        //send results to sink
        finalResults.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .build());

        env.execute("Flink Skyline: " + algo);
    }

    //LOCAL SKYLINE AGGREGATOR
    public static class SkylineLocalProcessor extends CoProcessFunction<ServiceTuple, Tuple2<Integer, String>, Tuple2<String, List<ServiceTuple>>> {

        private transient ListState<ServiceTuple> localSkylineState;
        //buffer for incoming points to reduce State I/O
        private transient List<ServiceTuple> inputBuffer;
        private final int BUFFER_SIZE = 10000; // Tune this based on your 16GB RAM

        @Override
        public void open(Configuration config) {
            localSkylineState = getRuntimeContext().getListState(new ListStateDescriptor<>("localSky", ServiceTuple.class));
            inputBuffer = new ArrayList<>();
        }

        @Override
        public void processElement1(ServiceTuple point, Context ctx, Collector<Tuple2<String, List<ServiceTuple>>> out) throws Exception {
            //add to temporary memory buffer
            inputBuffer.add(point);

            //only access State when buffer is full
            if (inputBuffer.size() >= BUFFER_SIZE) {
                processBuffer();
            }
        }

        @Override
        public void processElement2(Tuple2<Integer, String> trigger, Context ctx, Collector<Tuple2<String, List<ServiceTuple>>> out) throws Exception {
            //flush buffer before answering query to ensure consistency
            if (!inputBuffer.isEmpty()) {
                processBuffer();
            }
            String queryId = trigger.f1;
            List<ServiceTuple> results = new ArrayList<>();
            for (ServiceTuple s : localSkylineState.get()) {
                results.add(s);
            }
            out.collect(new Tuple2<>(queryId, results));
        }

        private void processBuffer() throws Exception {
            //load state ONCE
            Iterable<ServiceTuple> stateIter = localSkylineState.get();
            List<ServiceTuple> currentSkyline = new ArrayList<>();
            if (stateIter != null) {
                for (ServiceTuple s : stateIter) currentSkyline.add(s);
            }

            //process all buffered points against the current skyline
            for (ServiceTuple candidate : inputBuffer) {
                boolean isDominated = false;
                // Standard BNL logic
                Iterator<ServiceTuple> it = currentSkyline.iterator();
                while (it.hasNext()) {
                    ServiceTuple existing = it.next();
                    if (existing.dominates(candidate)) {
                        isDominated = true;
                        break;
                    }
                    if (candidate.dominates(existing)) {
                        it.remove(); //remove dominated existing point
                    }
                }
                if (!isDominated) {
                    currentSkyline.add(candidate);
                }
            }
            //write state back ONCE
            localSkylineState.update(currentSkyline);
            inputBuffer.clear();
        }
    }

    //GLOBAL AGGREGATOR
    public static class GlobalSkylineAggregator extends KeyedProcessFunction<String, Tuple2<String, List<ServiceTuple>>, String> {

        private final int totalPartitions;
        private transient ValueState<List<ServiceTuple>> globalBuffer;
        private transient ValueState<Integer> arrivedCount;

        public GlobalSkylineAggregator(int totalPartitions) {
            this.totalPartitions = totalPartitions;
        }

        @Override
        public void open(Configuration config) {
            globalBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>("gBuffer", TypeInformation.of(new TypeHint<List<ServiceTuple>>() {})));
            arrivedCount = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Integer.class));
        }

        @Override
        public void processElement(Tuple2<String, List<ServiceTuple>> input, Context ctx, Collector<String> out) throws Exception {
            List<ServiceTuple> currentGlobal = globalBuffer.value();
            if (currentGlobal == null) currentGlobal = new ArrayList<>();

            Integer count = arrivedCount.value();
            if (count == null) count = 0;

            List<ServiceTuple> incoming = input.f1;

            if (!incoming.isEmpty()) {
                for (ServiceTuple candidate : incoming) {
                    boolean isDominated = false;
                    Iterator<ServiceTuple> it = currentGlobal.iterator();
                    while (it.hasNext()) {
                        ServiceTuple existing = it.next();
                        if (existing.dominates(candidate)) { isDominated = true; break; }
                        if (candidate.dominates(existing)) it.remove();
                    }
                    if (!isDominated) currentGlobal.add(candidate);
                }
            }

            globalBuffer.update(currentGlobal);
            arrivedCount.update(count + 1);

            //throwing each element into the topic separately so that Kafka's memory doesn't explode.
            if (count + 1 >= totalPartitions) {
                String qId = ctx.getCurrentKey();

                //current results are temporary in json in case we want to make a script
                //to visualize them
                //temporary prints
                 out.collect("\n\n--- Computed Skyline: " + qId + " ---");
                 out.collect("{\"type\":\"META\", \"id\":\"" + qId + "\", \"count\":" + currentGlobal.size() + "}");

                for(ServiceTuple s : currentGlobal) {
                    //temporary json format
                    String json = "{\"id\":\"" + qId + "\"," +
                            "\"point\":" + Arrays.toString(s.values) +
                            "}";
                    out.collect(json);
                }
                globalBuffer.clear();
                arrivedCount.clear();
            }
        }
    }

    //PARTITIONERS
    public static class PartitioningLogic implements Serializable {
        public interface SkylinePartitioner extends KeySelector<ServiceTuple, Integer> { }
        public static class DimPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double maxVal;
            public DimPartitioner(int partitions, double maxVal) { this.partitions = partitions; this.maxVal = maxVal; }
            @Override
            public Integer getKey(ServiceTuple t) {
                int p = (int) (t.values[0] / (maxVal / partitions));
                return Math.min(p, partitions - 1);
            }
        }

        public static class AnglePartitioner implements SkylinePartitioner {
            private final int partitions;
            public AnglePartitioner(int partitions) { this.partitions = partitions; }
            @Override
            public Integer getKey(ServiceTuple t) {
                double sumSq = 0;
                for(double v : t.values) sumSq += v*v;
                double r = Math.sqrt(sumSq);
                if (r == 0) return 0;
                double phi = Math.acos(t.values[0] / r);
                double totalAngle = Math.PI / 2.0;
                int p = (int) (phi / (totalAngle / partitions));
                return Math.min(p, partitions - 1);
            }
        }

        public static class GridPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double maxVal;
            //assume dimensions are split in half according to the paper
            public GridPartitioner(int partitions, double maxVal, int dims) {
                this.partitions = partitions;
                this.maxVal = maxVal;
            }
            @Override
            public Integer getKey(ServiceTuple t) {
                int gridIndex = 0;
                double threshold = maxVal / 2.0; //paper specifies "half value of the maximum"

                //we need a logic to prune the regions that will be dominated by another.
                //create a unique integer ID based on spatial location (Bitwise encoding)
                for (int i = 0; i < t.values.length; i++) {
                    // If the value is in the "upper" (worse) half of this dimension, set the bit
                    if (t.values[i] >= threshold) {
                        gridIndex |= (1 << i);
                    }
                }

                // Return the raw Grid Index if you want distinct processing for every cell,
                // OR modulo it if you must limit to 'partitions' count.
                // To strictly follow the paper's ability to prune specific grids (like the top-right),
                // we ideally want the gridIndex itself. However, to map to Flink parallelism:
                return gridIndex % partitions;
            }
        }
    }
}
