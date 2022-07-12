package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.join.*;
import uni.freiburg.sparqljoin.util.Hasher;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements hash join algorithm
 */
public class HashJoin implements AbstractJoin {
    private static final Logger LOG = LoggerFactory.getLogger(HashJoin.class);

    @Override
    public ComplexTable join(ComplexTable R, ComplexTable S, int joinPropertyR, JoinOn joinOnR, int joinPropertyS, JoinOn joinOnS) {
        // Use the smaller relation of R and S as the build relation. Algorithm will run faster
        if (R.getValues().size() < S.getValues().size()) {
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
    public HashJoinBuildOutput build(ComplexTable table, int property, JoinOn joinOn) {
        LOG.info("Starting build phase");

        HashMap<Integer, List<JoinedItems>> buildOutput = new HashMap<>();
        table.getValues().forEach(joinedItems -> {
            Item item = joinedItems.values().get(property);
            if (item == null) {
                return;
            }

            int key = joinOn == JoinOn.SUBJECT ? item.subject() : item.object();
            int hashedKey = Hasher.hash(key);

            if (buildOutput.containsKey(hashedKey)) {
                List<JoinedItems> list = buildOutput.get(hashedKey);
                list.add(joinedItems);
                buildOutput.put(hashedKey, list); // TODO is this required? buildOutput.get() returns a reference
            } else {
                buildOutput.put(hashedKey, new ArrayList<>(List.of(joinedItems)));
            }
        });
        return new HashJoinBuildOutput(buildOutput);
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
    public ComplexTable probe(BuildOutput partitions, ComplexTable R, ComplexTable S,
                              int joinPropertyR, JoinOn joinOnR,
                              int joinPropertyS, JoinOn joinOnS) {
        LOG.info("Starting probe phase");

        HashMap<Integer, List<JoinedItems>> hashedReferenceTablePartitions = ((HashJoinBuildOutput) partitions).getPartition();

        Dictionary referenceTableDictionary = R.getObjectDictionary();
        Dictionary probeTableDictionary = S.getObjectDictionary();
        Dictionary probeTablePropertyDictionary = S.getPropertyDictionary();

        // Output variables
        Dictionary outputObjectDictionary = referenceTableDictionary.clone();
        Dictionary outputPropertyDictionary = R.getPropertyDictionary().clone();
        List<JoinedItems> joinedItems = new ArrayList<>();

        AtomicInteger numProcessedRecords = new AtomicInteger(0);
        int numRecords = S.getValues().size();
        AtomicBoolean printedProgress = new AtomicBoolean(false);

        // For each tuple in S...
        S.getValues().forEach(probeJoinedItems -> {
            if (numProcessedRecords.incrementAndGet() % 10000 == 0 && !printedProgress.get()) {
                System.out.println("Iterate over S tuples. " + numProcessedRecords + "/" + numRecords + " records = " + (int)(100f * numProcessedRecords.get() / numRecords) + "%");
                printedProgress.set(true);
            } else {
                printedProgress.set(false);
            }

            int hashedSKey;
            if (joinOnS == JoinOn.SUBJECT) {
                hashedSKey = Hasher.hash(probeJoinedItems.subject());
            } else {
                hashedSKey = Hasher.hash(probeJoinedItems.values().get(joinPropertyS).object());
            }

            // ... look up its join key in the hash table of R
            if (hashedReferenceTablePartitions.containsKey(hashedSKey)) {
                for (JoinedItems referenceJoinedItems : hashedReferenceTablePartitions.get(hashedSKey)) {
                    if (!referenceJoinedItems.values().containsKey(joinPropertyR)) {
                        return; // Discard this S tuple
                    }
                    if (!probeJoinedItems.values().containsKey(joinPropertyS)) {
                        return; // Discard this S tuple
                    }

                    // A match is found. Output the combined tuple.

                    // Get join property
                    Item referenceItem = referenceJoinedItems.values().get(joinPropertyR);
                    Item probeItem = probeJoinedItems.values().get(joinPropertyS);

                    // Check if there is a hash collision
                    int referenceJoinKey = joinOnR == JoinOn.SUBJECT ? referenceItem.subject() : referenceItem.object();
                    int probeJoinKey = joinOnS == JoinOn.SUBJECT ? probeItem.subject() : probeItem.object();
                    if (referenceJoinKey == probeJoinKey) {
                        // No hash collision
                        mergeTuplesAndDictionaries(joinedItems, referenceJoinedItems, probeJoinedItems);
                    }
                }
            }
        });

        return new ComplexTable(outputPropertyDictionary, outputObjectDictionary, new PropertyValues<>(joinedItems));
    }

}
