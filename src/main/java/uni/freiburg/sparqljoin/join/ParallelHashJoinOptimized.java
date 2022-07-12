package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.Item;
import uni.freiburg.sparqljoin.model.db.PropertyValues;
import uni.freiburg.sparqljoin.model.db.VerticallyPartitionedTable;
import uni.freiburg.sparqljoin.model.join.HashJoinBuildOutputOptimized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ParallelHashJoinOptimized extends HashJoinOptimized {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHashJoinOptimized.class);

    private int numThreads;
    private final ExecutorService threadPool;
    private final ExecutorService cachedPool;

    public ParallelHashJoinOptimized() {
        // each core has 2 threads
        this.numThreads = 8;// Runtime.getRuntime().availableProcessors()*2;
        // thread pool for partitions
        this.threadPool = Executors.newFixedThreadPool(numThreads);
        // thread pool for misc tasks
        this.cachedPool = Executors.newCachedThreadPool();
    }

    private List<VerticallyPartitionedTable> splitRelationIntoParts(VerticallyPartitionedTable relation, String joinProperty, int numParts) {
        // initialize array with number of partitions
        List<VerticallyPartitionedTable> relationParts = new ArrayList<>(numParts);
        // fill array with partitions
        for (int i = 0; i < numParts; i++) {
            relationParts.add(new VerticallyPartitionedTable(relation.dictionaries(), new HashMap<>()));
        }

        int totalElements = relation.propertyItems().get(joinProperty).getValues().size();
        // for each element in the relation put it in the corresponding partition
        for (int numProcessedElements = 0; numProcessedElements < totalElements; numProcessedElements++) {
            // find needed partition
            HashMap<String, PropertyValues<Item>> partitionedValues = relationParts.get(numProcessedElements % numParts).propertyItems();
            // add element to corresponding partition
            int index = numProcessedElements;
            relation.propertyItems().forEach(((property, propertyValues) -> {
                Item item = propertyValues.getValues().get(index);
                PropertyValues<Item> values = partitionedValues.get(property);
                if (values == null) {
                    partitionedValues.put(property, new PropertyValues<>(new ArrayList<>(List.of(item))));
                } else {
                    values.put(item);
                }
            }));
        }

        return relationParts;
    }

    @Override
    public VerticallyPartitionedTable join(VerticallyPartitionedTable R, VerticallyPartitionedTable S,
                                           String joinPropertyR, JoinOn joinOnR,
                                           String joinPropertyS, JoinOn joinOnS) {
        // TODO clean code

        if (numThreads > R.size()) numThreads = R.size();
        if (numThreads > S.size()) numThreads = S.size();

        // partition R and S in parallel
        var buildRelationPartsFeature = cachedPool.submit(() -> splitRelationIntoParts(R, joinPropertyR, numThreads));
        var probeRelationPartsFeature = cachedPool.submit(() -> splitRelationIntoParts(S, joinPropertyS, numThreads));
        // Partition relation R across all threads for the build phase
        List<VerticallyPartitionedTable> buildRelationParts = null;
        try {
            buildRelationParts = buildRelationPartsFeature.get();
        } catch (InterruptedException e) {
            LOG.error("Interruption error of pre partitions: {}", e.toString());
        } catch (ExecutionException e) {
            LOG.error("Execution error of pre partitions: {}", e.toString());
        }

        // Start build features
        List<Future<HashJoinBuildOutputOptimized>> buildFeatures = buildRelationParts.stream()
                .map(part -> threadPool.submit(new ParallelHashJoinBuildWorker(this, part, joinPropertyR, joinOnR)))
                .toList();

        // Wait for build threads to finish and get results
        List<HashJoinBuildOutputOptimized> buildOutputs = new ArrayList<>();
        for (var feature : buildFeatures) {
            try {
                buildOutputs.add(feature.get());
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException occurred while building the relation in thread: {}", e.toString());
            } catch (ExecutionException e) {
                LOG.warn("ExecutionException occurred while building the relation in thread: {}", e.toString());
            }
        }

        // Combine build thread results
        HashJoinBuildOutputOptimized combinedBuildOutput = new HashJoinBuildOutputOptimized(new HashMap<>());
        for (int i = 0; i < buildOutputs.size(); i++) {
            combinedBuildOutput.mergeFrom(buildOutputs.get(i), i, numThreads);
        }

        // Partition relation S across all threads for the probe phase
        List<VerticallyPartitionedTable> probeRelationParts = null;
        try {
            probeRelationParts = probeRelationPartsFeature.get();
        } catch (InterruptedException e) {
            LOG.error("Interruption error of pre partitions: {}", e.toString());
        } catch (ExecutionException e) {
            LOG.error("Execution error of pre partitions: {}", e.toString());
        }

        // Start probe features
        List<Future<VerticallyPartitionedTable>> probeFeatures = probeRelationParts.stream()
                .map(part -> threadPool.submit(new ParallelHashJoinProbeWorker(this, combinedBuildOutput, R, part, joinPropertyR, joinOnR, joinPropertyS, joinOnS)))
                .toList();

        // Wait for probe threads to finish and get results
        List<VerticallyPartitionedTable> probeOutputs = new ArrayList<>();
        for (var feature : probeFeatures) {
            try {
                probeOutputs.add(feature.get());
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException occurred while building the relation in thread: {}", e.toString());
            } catch (ExecutionException e) {
                LOG.warn("ExecutionException occurred while building the relation in thread: {}", e.toString());
            }
        }

        // TODO: Combine probe results in parallel
        VerticallyPartitionedTable joinResult = probeOutputs.get(0);
        for (int i = 1; i < numThreads; i++) {
            joinResult.putAll(probeOutputs.get(i));
        }

        return joinResult;
    }
}
