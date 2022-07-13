package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.join.*;
import uni.freiburg.sparqljoin.util.Hasher;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements hash join algorithm with vertically partitioned ouput
 */
public class HashJoinOptimized implements AbstractJoinOptimized {
    private static final Logger LOG = LoggerFactory.getLogger(HashJoinOptimized.class);

    @Override
    public VerticallyPartitionedTable join(VerticallyPartitionedTable R, VerticallyPartitionedTable S,
                                           String joinPropertyR, JoinOn joinOnR,
                                           String joinPropertyS, JoinOn joinOnS) {
        // Use the smaller relation of R and S as the build relation. Algorithm will run faster
        if (R.propertyItems().get(joinPropertyR).getValues().size()
                < S.propertyItems().get(joinPropertyS).getValues().size()) {
            BuildOutput output = build(R, joinPropertyR, joinOnR);
            return probe(output, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);
        } else {
            BuildOutput output = build(S, joinPropertyS, joinOnS);
            return probe(output, S, R, joinPropertyS, joinOnS, joinPropertyR, joinOnR);
        }
    }

    /**
     * Build a hash map partition over the join key:
     * for each item in relation R, calculate the hash of the join key and append the JoinedItems instance to the partition corresponding to the hashed join key
     *
     * @param table    input relation
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @return build output - HashMap with key = hashed join key, value = list of JoinedItems
     */
    @Override
    public HashJoinBuildOutputOptimized build(VerticallyPartitionedTable table, String property, JoinOn joinOn) {
        LOG.info("Starting build phase");

        HashMap<Integer, List<Integer>> buildOutput = new HashMap<>();
        List<Item> items = table.propertyItems().get(property).getValues();
        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);
            if (item == null) {
                continue;
            }

            int key = joinOn == JoinOn.SUBJECT ? item.subject() : item.object();
            int hashedKey = Hasher.hash(key);

            // put hashed key and index of the item to the partition
            if (buildOutput.containsKey(hashedKey)) {
                List<Integer> list = buildOutput.get(hashedKey);
                list.add(i);
            } else {
                buildOutput.put(hashedKey, new ArrayList<>(List.of(i)));
            }
        }
        return new HashJoinBuildOutputOptimized(buildOutput);
    }

    /**
     * Probe join key from the joining table:
     * for each item in probe table
     * take hash of the join key subject
     * find matching bucket hash map partition
     * compare and join items in the matching bucket
     *
     * @param partitions    build relation partitions from the build phase
     * @param R             R relation table - build relation
     * @param S             S relation table - probe relation
     * @param joinPropertyR name of the property to join on from table R (build relation)
     * @param joinOnR       join field in property from R (build relation)
     * @param joinPropertyS name of the property to join on from table S (probe relation)
     * @param joinOnS       join field in property from S (probe relation)
     * @return new joined table
     */
    @Override
    public VerticallyPartitionedTable probe(BuildOutput partitions, VerticallyPartitionedTable R, VerticallyPartitionedTable S,
                              String joinPropertyR, JoinOn joinOnR,
                              String joinPropertyS, JoinOn joinOnS) {
        LOG.info("Starting probe phase");

        HashMap<Integer, List<Integer>> hashedReferenceTablePartitions = ((HashJoinBuildOutputOptimized) partitions).getPartition();

        HashMap<String, PropertyValues<Item>> joinedItems = new HashMap<>();

        List<Item> referenceItems = R.propertyItems().get(joinPropertyR).getValues();
        List<Item> probeItems = S.propertyItems().get(joinPropertyS).getValues();

        AtomicInteger numProcessedRecords = new AtomicInteger(0);
        int numRecords = probeItems.size();
        AtomicBoolean printedProgress = new AtomicBoolean(false);

        // For each tuple in S...
        for (int probeItemIndex = 0; probeItemIndex < numRecords; probeItemIndex++) {
            if (numProcessedRecords.incrementAndGet() % 10000 == 0 && !printedProgress.get()) {
                System.out.println("Iterate over S tuples. Joined results size = " + joinedItems.get(joinPropertyR).getValues().size() + " " + numProcessedRecords + "/" + numRecords + " records = " + (int)(100f * numProcessedRecords.get() / numRecords) + "%");
                printedProgress.set(true);
            } else {
                printedProgress.set(false);
            }
            Item probeItem = probeItems.get(probeItemIndex);
            int hashedSKey;
            if (joinOnS == JoinOn.SUBJECT) {
                hashedSKey = Hasher.hash(probeItem.subject());
            } else {
                hashedSKey = Hasher.hash(probeItem.object());
            }

            // ... look up its join key in the hash table of R
            if (hashedReferenceTablePartitions.containsKey(hashedSKey)) {
                for (int referenceItemIndex: hashedReferenceTablePartitions.get(hashedSKey)) {
                    // Get reference item
                    Item referenceItem = referenceItems.get(referenceItemIndex);
                    if (referenceItem == null) {
                        continue;
                    }

                    // A match is found. Output the combined tuple.

                    // Check if there is a hash collision
                    int referenceJoinKey = joinOnR == JoinOn.SUBJECT ? referenceItem.subject() : referenceItem.object();
                    int probeJoinKey = joinOnS == JoinOn.SUBJECT ? probeItem.subject() : probeItem.object();
                    if (referenceJoinKey == probeJoinKey) {
                        // No hash collision
                        mergeTuples(joinedItems, referenceItemIndex, R, probeItemIndex, S);
                    } else {
                        if (numProcessedRecords.get() % 1000 == 0 && !printedProgress.get()) {
                            System.out.println("hash collision : referenceKey = " + referenceJoinKey + ", probeKey = " + probeJoinKey
                                    + ", joinPropertyR = " + joinPropertyR + ", joinPropertyS = " + joinPropertyS
                                    + " . " + numProcessedRecords + "/" + numRecords + " records = " + (int)(100f * numProcessedRecords.get() / numRecords) + "%");
                        }
                    }
                }
            }
        }

        return R.join(S, joinedItems);
    }

}