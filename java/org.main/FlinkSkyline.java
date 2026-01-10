package org.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class FlinkSkyline {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // --- Parameters ---
        final int parallelism = params.getInt("parallelism", 4);
        final String algo = params.get("algo", "mr-angle").toLowerCase();
        final String inputTopic = params.get("input-topic", "input-tuples");
        final String queryTopic = params.get("query-topic", "queries");
        final String outputTopic = params.get("output-topic", "output-skyline");
        final double domainMax = params.getDouble("domain", 1000.0);
        final int dims = params.getInt("dims", 2);

        // Empirically partitions set to 2x number of nodes (parallelism)
        final int numPartitions = 2 * parallelism;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // --- Kafka Sources ---
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

        // 1. Ingest and Parse
        DataStream<ServiceTuple> rawData = env.fromSource(tupleSrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Data")
                .map(ServiceTuple::fromString)
                .filter(Objects::nonNull);

        // 2. Partitioning Logic
        DataStream<ServiceTuple> processedData = rawData;
        PartitioningLogic.SkylinePartitioner partitioner;

        switch (algo) {
            case "mr-dim":
                // MR-Dim: Standard dimensional partitioning
                partitioner = new PartitioningLogic.DimPartitioner(numPartitions, domainMax);
                break;
            case "mr-grid":
                // MR-Grid: Prune dominated grids FIRST
                //processedData = rawData.filter(new PartitioningLogic.GridDominanceFilter(domainMax, dims));
                partitioner = new PartitioningLogic.GridPartitioner(numPartitions, domainMax, dims);
                break;
            default:
                // MR-Angle: Hyperspherical partitioning
                partitioner = new PartitioningLogic.AnglePartitioner(numPartitions, dims);
                break;
        }

        KeyedStream<ServiceTuple, Integer> keyedData = processedData.keyBy(partitioner);

        // 3. Query Trigger Stream
        KeyedStream<Tuple3<Integer, String, Long>, Integer> keyedTriggers = env
                .fromSource(querySrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Queries")
                .flatMap(new FlatMapFunction<String, Tuple3<Integer, String, Long>>() {
                    @Override
                    public void flatMap(String rawPayload, Collector<Tuple3<Integer, String, Long>> out) {
                        long startTime = System.currentTimeMillis();
                        // Broadcast query to all partitions
                        for (int i = 0; i < numPartitions; i++) {
                            out.collect(new Tuple3<>(i, rawPayload, startTime));
                        }
                    }
                })
                .keyBy(t -> t.f0);

        // 4. Local Processing (Connect Data & Queries)
        // UPDATE: Changed type to Tuple6
        DataStream<Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long>> localSkylines = keyedData
                .connect(keyedTriggers)
                .process(new SkylineLocalProcessor())
                .name("LocalSkylineProcessor");

        // 5. Global Aggregation
        DataStream<String> finalResults = localSkylines
                .keyBy(t -> t.f1) // <--- CRITICAL FIX: Use f1 (Query String) as key, not f0 (Partition Integer)
                .process(new GlobalSkylineAggregator(numPartitions))
                .name("GlobalReducer");

        // 6. Sink
        finalResults.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setProperty("max.request.size", "10485760")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .build());

        env.execute("Flink Skyline: " + algo);
    }

    // ------------------------------------------------------------------------
    // LOCAL PROCESSOR (BNL Algorithm)
    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------
    // LOCAL PROCESSOR (BNL Algorithm) with BARRIER SYNC
    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------
    // LOCAL PROCESSOR (BNL Algorithm)
    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------
    // LOCAL PROCESSOR (BNL Algorithm)
    // ------------------------------------------------------------------------
    // Output Updated: Tuple6 <PartitionID, QueryPayload, TriggerTime, PartitionStartTime, SkylineList, LocalCPUTime>
    public static class SkylineLocalProcessor extends CoProcessFunction<ServiceTuple, Tuple3<Integer, String, Long>, Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long>> {

        private transient ListState<ServiceTuple> localSkylineState;
        private transient List<ServiceTuple> inputBuffer;

        // --- Synchronization State ---
        private transient ValueState<Long> maxSeenIdState;
        private transient ListState<Tuple3<Integer, String, Long>> pendingQueriesState;
        private transient ValueState<Long> startTimeState;
        // Track accumulated CPU time for the algorithm
        private transient ValueState<Long> accumulatedCpuNanosState;

        private final int BUFFER_SIZE = 5000;

        @Override
        public void open(Configuration config) {
            localSkylineState = getRuntimeContext().getListState(new ListStateDescriptor<>("localSky", ServiceTuple.class));
            inputBuffer = new ArrayList<>();
            startTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("jobStartTime", Long.class));
            maxSeenIdState = getRuntimeContext().getState(new ValueStateDescriptor<>("maxId", Long.class));
            pendingQueriesState = getRuntimeContext().getListState(new ListStateDescriptor<>("pendingQs", TypeInformation.of(new TypeHint<Tuple3<Integer, String, Long>>() {})));
            accumulatedCpuNanosState = getRuntimeContext().getState(new ValueStateDescriptor<>("cpuTime", Long.class));
        }

        @Override
        public void processElement1(ServiceTuple point, Context ctx, Collector<Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long>> out) throws Exception {
            // Start Timer
            long startNano = System.nanoTime();

            if (startTimeState.value() == null) {
                startTimeState.update(System.currentTimeMillis());
            }

            long currentId = Long.parseLong(point.id);
            Long maxIdWrapper = maxSeenIdState.value();
            long maxId = (maxIdWrapper != null) ? maxIdWrapper : -1L;

            if (currentId > maxId) {
                maxSeenIdState.update(currentId);
                maxId = currentId;
            }

            inputBuffer.add(point);
            if (inputBuffer.size() >= BUFFER_SIZE) {
                processBuffer();
            }

            // End Timer & Update State
            long duration = System.nanoTime() - startNano;
            Long currentCpu = accumulatedCpuNanosState.value();
            accumulatedCpuNanosState.update((currentCpu == null ? 0 : currentCpu) + duration);

            // Check Barrier
            Iterable<Tuple3<Integer, String, Long>> pending = pendingQueriesState.get();
            if (pending != null) {
                List<Tuple3<Integer, String, Long>> remaining = new ArrayList<>();
                boolean processedAny = false;
                for (Tuple3<Integer, String, Long> query : pending) {
                    String[] parts = query.f1.split(",");
                    long requiredCount = (parts.length > 1) ? Long.parseLong(parts[1]) : 0;
                    if (maxId >= requiredCount) {
                        processQuery(query, out);
                        processedAny = true;
                    } else {
                        remaining.add(query);
                    }
                }
                if (processedAny) pendingQueriesState.update(remaining);
            }
        }

        @Override
        public void processElement2(Tuple3<Integer, String, Long> trigger, Context ctx, Collector<Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long>> out) throws Exception {
            String[] parts = trigger.f1.split(",");
            long requiredCount = (parts.length > 1) ? Long.parseLong(parts[1]) : 0;
            Long maxIdWrapper = maxSeenIdState.value();
            long currentMaxId = (maxIdWrapper != null) ? maxIdWrapper : -1L;

            //int pId = trigger.f0;
            //System.out.println(">>> Partition " + pId + " Query Trigger. CurrentMaxID: " + currentMaxId + " | Required: " + requiredCount);

            if (currentMaxId >= requiredCount || currentMaxId == -1L) {
                processQuery(trigger, out);
            } else {
                pendingQueriesState.add(trigger);
            }
        }

        private void processQuery(Tuple3<Integer, String, Long> trigger, Collector<Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long>> out) throws Exception {
            // Start Timer for any remaining flush
            long startNano = System.nanoTime();
            if (!inputBuffer.isEmpty()) {
                processBuffer();
            }
            long duration = System.nanoTime() - startNano;
            Long currentCpu = accumulatedCpuNanosState.value();
            long totalCpuNanos = (currentCpu == null ? 0 : currentCpu) + duration;
            accumulatedCpuNanosState.update(totalCpuNanos);

            // Prepare Output
            int partitionId = trigger.f0; // Identify which partition this is
            String queryPayload = trigger.f1;
            Long triggerDispatchTime = trigger.f2;
            Long partitionStartTime = startTimeState.value();
            if (partitionStartTime == null) partitionStartTime = System.currentTimeMillis();

            List<ServiceTuple> results = new ArrayList<>();
            for (ServiceTuple s : localSkylineState.get()) {
                // VITAL: Mark where this tuple came from for Optimality Calculation
                s.originPartition = partitionId;
                results.add(s);
            }

            long totalCpuMillis = totalCpuNanos / 1_000_000L;

            out.collect(new Tuple6<>(
                    partitionId,
                    queryPayload,
                    triggerDispatchTime,
                    partitionStartTime,
                    results,
                    totalCpuMillis
            ));
        }

        private void processBuffer() throws Exception {
            Iterable<ServiceTuple> stateIter = localSkylineState.get();
            List<ServiceTuple> currentSkyline = new ArrayList<>();
            if (stateIter != null) {
                for (ServiceTuple s : stateIter) currentSkyline.add(s);
            }

            for (ServiceTuple candidate : inputBuffer) {
                boolean isDominated = false;
                Iterator<ServiceTuple> it = currentSkyline.iterator();
                while (it.hasNext()) {
                    ServiceTuple existing = it.next();
                    if (existing.dominates(candidate)) {
                        isDominated = true;
                        break;
                    }
                    if (candidate.dominates(existing)) {
                        it.remove();
                    }
                }
                if (!isDominated) {
                    currentSkyline.add(candidate);
                }
            }
            localSkylineState.update(currentSkyline);
            inputBuffer.clear();
        }
    }

    // ------------------------------------------------------------------------
    // GLOBAL AGGREGATOR
    // ------------------------------------------------------------------------
    // Input Updated: Tuple6 <PartitionID, QueryPayload, TriggerTime, PartitionStartTime, SkylineList, LocalCPUTime>
    public static class GlobalSkylineAggregator extends KeyedProcessFunction<String, Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long>, String> {

        private final int totalPartitions;
        private transient ValueState<List<ServiceTuple>> globalBuffer;
        private transient ValueState<Integer> arrivedCount;
        private transient ValueState<Long> minStartTimeState;
        private transient ValueState<Long> lastArrivalState;
        private transient ValueState<Long> maxLocalCpuState;

        // NEW: Map to store the size of the local skyline for each partition (for Optimality Calc)
        private transient MapState<Integer, Integer> localSkylineSizes;

        public GlobalSkylineAggregator(int totalPartitions) {
            this.totalPartitions = totalPartitions;
        }

        @Override
        public void open(Configuration config) {
            globalBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>("gBuffer", TypeInformation.of(new TypeHint<List<ServiceTuple>>() {})));
            arrivedCount = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Integer.class));
            minStartTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("minStart", Long.class));
            lastArrivalState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastArr", Long.class));
            maxLocalCpuState = getRuntimeContext().getState(new ValueStateDescriptor<>("maxCpu", Long.class));
            localSkylineSizes = getRuntimeContext().getMapState(new MapStateDescriptor<>("localSizes", Integer.class, Integer.class));
        }

        @Override
        public void processElement(Tuple6<Integer, String, Long, Long, List<ServiceTuple>, Long> input, Context ctx, Collector<String> out) throws Exception {
            List<ServiceTuple> currentGlobal = globalBuffer.value();
            if (currentGlobal == null) currentGlobal = new ArrayList<>();

            Integer count = arrivedCount.value();
            if (count == null) count = 0;

            // 1. Update Timing Stats
            Long incomingStartTime = input.f3;
            Long currentMinStart = minStartTimeState.value();
            if (currentMinStart == null || (incomingStartTime != null && incomingStartTime < currentMinStart)) {
                minStartTimeState.update(incomingStartTime);
            }

            long now = System.currentTimeMillis();
            lastArrivalState.update(now);

            Long incomingCpu = input.f5;
            Long currentMaxCpu = maxLocalCpuState.value();
            if (currentMaxCpu == null || incomingCpu > currentMaxCpu) {
                maxLocalCpuState.update(incomingCpu);
            }

            // 2. Store Local Size for Optimality Calculation
            int partitionId = input.f0;
            List<ServiceTuple> incoming = input.f4;
            localSkylineSizes.put(partitionId, incoming.size());

            // 3. Merge Logic
            if (incoming != null && !incoming.isEmpty()) {
                for (ServiceTuple candidate : incoming) {
                    boolean isDominated = false;
                    Iterator<ServiceTuple> it = currentGlobal.iterator();
                    while (it.hasNext()) {
                        ServiceTuple existing = it.next();
                        if (existing.dominates(candidate)) {
                            isDominated = true;
                            break;
                        }
                        if (candidate.dominates(existing)) {
                            it.remove();
                        }
                    }
                    if (!isDominated) {
                        currentGlobal.add(candidate);
                    }
                }
            }

            globalBuffer.update(currentGlobal);
            arrivedCount.update(count + 1);

            // 4. Final Emission
            if (count + 1 >= totalPartitions) {
                long jobFinishTime = System.currentTimeMillis();
                Long jobStartTime = minStartTimeState.value();
                Long mapFinishTime = lastArrivalState.value();
                Long maxLocalCpu = maxLocalCpuState.value();

                // --- A. Timing Metrics (Objective 2, 4, 5) ---
                long mapWallTime = (jobStartTime != null) ? (mapFinishTime - jobStartTime) : 0;
                long localProcessingTime = (maxLocalCpu != null) ? maxLocalCpu : 0;
                long ingestionTime = mapWallTime - localProcessingTime;
                if (ingestionTime < 0) ingestionTime = 0;

                long globalProcessingTime = jobFinishTime - mapFinishTime;
                long totalProcessingTime = (jobStartTime != null) ? (jobFinishTime - jobStartTime) : 0;
                long queryLatency = jobFinishTime - input.f2; // trigger time from input

                // --- B. Optimality Metric (Objective 3) ---
                java.util.Map<Integer, Integer> survivors = new java.util.HashMap<>();
                for(ServiceTuple s : currentGlobal) {
                    survivors.put(s.originPartition, survivors.getOrDefault(s.originPartition, 0) + 1);
                }

                double sumRatios = 0.0;
                for (int i = 0; i < totalPartitions; i++) {
                    if(localSkylineSizes.contains(i)) {
                        int localSize = localSkylineSizes.get(i);
                        int survivorCount = survivors.getOrDefault(i, 0);
                        if (localSize > 0) {
                            sumRatios += (double) survivorCount / localSize;
                        }
                    }
                }
                double optimality = sumRatios / totalPartitions;

                //remove comment if you want to visualise the points(its crashes if data>2mil)
                // --- C. Visualization Data (Objective 1) ---
//                StringBuilder pointsJson = new StringBuilder("[");
//                for (int i = 0; i < currentGlobal.size(); i++) {
//                    ServiceTuple s = currentGlobal.get(i);
//                    pointsJson.append("[");
//                    for(int j=0; j<s.values.length; j++) {
//                        pointsJson.append(s.values[j]);
//                        if(j < s.values.length - 1) pointsJson.append(",");
//                    }
//                    pointsJson.append("]");
//                    if(i < currentGlobal.size() - 1) pointsJson.append(", ");
//                }
//                pointsJson.append("]");

                // --- JSON Output ---
                String payload = ctx.getCurrentKey();
                String[] parts = payload.split(",");
                String qId = parts[0];
                String recCount = (parts.length > 1) ? parts[1] : "unknown";

                StringBuilder sb = new StringBuilder();
                sb.append("{");
                sb.append("\"query_id\": \"").append(qId).append("\", ");
                sb.append("\"record_count\": ").append(recCount).append(", ");
                sb.append("\"skyline_size\": ").append(currentGlobal.size()).append(", ");
                sb.append("\"optimality\": ").append(String.format(java.util.Locale.US, "%.4f", optimality)).append(", ");

                sb.append("\"ingestion_time_ms\": ").append(ingestionTime).append(", ");
                sb.append("\"local_processing_time_ms\": ").append(localProcessingTime).append(", ");
                sb.append("\"global_processing_time_ms\": ").append(globalProcessingTime).append(", ");
                sb.append("\"total_processing_time_ms\": ").append(totalProcessingTime);//.append(", ");

                //sb.append("\"skyline_points\": ").append(pointsJson.toString());
                sb.append("}");

                out.collect(sb.toString());

                // Clear State
                globalBuffer.clear();
                arrivedCount.clear();
                lastArrivalState.clear();
                maxLocalCpuState.clear();
                localSkylineSizes.clear();
            }
        }
    }

    // ------------------------------------------------------------------------
    // PARTITIONING LOGIC (Corrected MR-Angle Logic)
    // ------------------------------------------------------------------------
    public static class PartitioningLogic implements Serializable {
        public interface SkylinePartitioner extends KeySelector<ServiceTuple, Integer> { }

        // --- MR-Dim ---
        public static class DimPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double maxVal;

            public DimPartitioner(int partitions, double maxVal) {
                this.partitions = partitions;
                this.maxVal = maxVal;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                int p = (int) (t.values[0] / (maxVal / partitions));
                return Math.max(0, Math.min(p, partitions - 1));
            }
        }


        // --- MR-Grid ---
//        public static class GridDominanceFilter extends RichFilterFunction<ServiceTuple> {
//            private final double threshold;
//            public GridDominanceFilter(double maxVal, int dims) {
//                this.threshold = maxVal / 2.0;
//            }
//            @Override
//            public boolean filter(ServiceTuple t) {
//                boolean allWorse = true;
//                for(double v : t.values) {
//                    if (v < threshold) {
//                        allWorse = false;
//                        break;
//                    }
//                }
//                return !allWorse;
//            }
//        }
//

        public static class GridPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double[] mids;

            public GridPartitioner(int partitions, double maxVal, int dims) {
                this.partitions = partitions;
                this.mids = new double[dims];
                // Pre-calculate midpoints (e.g. 5000 if domain is 10000)
                for (int i = 0; i < dims; i++) {
                    this.mids[i] = maxVal / 2.0;
                }
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                int mask = 0;
                // Works for ANY dimension count
                for (int i = 0; i < t.values.length; i++) {
                    // If dimension i is in the upper half, set bit i to 1
                    if (t.values[i] >= mids[i]) {
                        mask |= (1 << i);
                    }
                }
                // Map the Cell ID (mask) to a Worker ID
                return mask;
            }
        }
        // --- MR-Angle: Hyperspherical Partitioning (FIXED) ---
        public static class AnglePartitioner implements SkylinePartitioner {
            private final int partitions;
            private final int dims;

            public AnglePartitioner(int partitions, int dims) {
                this.partitions = partitions;
                this.dims = dims;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                // For N dimensions, there are N-1 angles
                int numAngles = dims - 1;

                // If 1D data (unlikely for skyline), return 0
                if (numAngles < 1) return 0;

                // 1. Calculate all angles phi_1 to phi_{n-1} based on Equation (1)
                // Note: Paper uses 1-based index. Java is 0-based.
                // phi_i corresponds to the angle between axis i and the rest of the vector.
                double[] angles = new double[numAngles];

                for (int i = 0; i < numAngles; i++) {
                    double v_i = t.values[i];

                    // Calculate magnitude of the remaining dimensions (v_{i+1} ... v_n)
                    double sumSqRest = 0.0;
                    for (int j = i + 1; j < dims; j++) {
                        sumSqRest += t.values[j] * t.values[j];
                    }
                    double hyp = Math.sqrt(sumSqRest);

                    // Calculate angle using atan2 (returns -pi to pi, but data is positive so 0 to pi/2)
                    angles[i] = Math.atan2(hyp, v_i);
                }

                // 2. Map the Angular Vector to a Partition ID
                // "Modify the grid partitioning over the n-1 subspaces"
                // We treat the angular coordinates as a new grid space and linearize it.

                // Determine splits per angular dimension to fit roughly into total partitions
                // If we have many dims and few partitions, we prioritize the first angles.
                // Simple linearization strategy:
                // Normalize angle to [0, 1) (dividing by PI/2) -> scale by splits -> compute index

                // Heuristic: Distribute cuts across dimensions.
                // For robustness with variable inputs, we use a mixed-radix-like hashing
                // or simply sum the weighted sectors to ensure all angles contribute.

                double maxAngle = Math.PI / 2.0;
                long linearizedID = 0;

                // We split the angular space into a grid.
                // We use a base-2 grid (high/low) for each angle if partitions allow,
                // or simply map the continuous angular values to the integer range.

                // Robust Implementation:
                // We map the multi-dimensional angular coordinate to a single linear value
                // by conceptually dividing the angular hypersphere into sectors.

                // Step A: Normalize all angles to 0.0 -> 1.0 range
                double normalizedSum = 0.0;
                for(int k=0; k < numAngles; k++) {
                    // weighted position: earlier angles often separate space more significantly in HS coords
                    normalizedSum += (angles[k] / maxAngle);
                }

                // Step B: Average normalized position to find "sector" in the linear sequence
                double avgPosition = normalizedSum / numAngles;

                // Step C: Map to partition range
                int p = (int) (avgPosition * partitions);

                // Alternative Rigorous Grid Implementation (if N is large enough):
                // If strictly following "Grid on Subspaces", we would do:
                // int id = 0;
                // for(double ang : angles) { if(ang > PI/4) id = (id << 1) | 1; else id = id << 1; }
                // return id % partitions;

                // Returning the mapping based on the aggregated angular position
                return Math.max(0, Math.min(p, partitions - 1));
            }
        }
    }
}
