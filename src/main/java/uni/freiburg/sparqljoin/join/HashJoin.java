package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.join.*;
import uni.freiburg.sparqljoin.util.Hasher;

import java.util.*;

/**
 * This class implements hash join algorithm
 */
public class HashJoin implements AbstractJoin {

    /**
     * Build a hash map partition over the join key:
     * for each item in relation R, calculate the hash of the join key and append the JoinedItems instance to the partition corresponding to the hashed join key
     *
     * @param table    input relation
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @return         build output - HashMap with key = hashed join key, value = list of JoinedItems
     */
    @Override
    public HashJoinBuildOutput build(ComplexTable table, String property, JoinOn joinOn) {
        HashMap<Long, List<JoinedItems>> buildOutput = new HashMap<>();
        table.getValues().forEach(joinedItems -> {
            if (!joinedItems.values().containsKey(property)) {
                return;
            }
            Item<Integer> item = joinedItems.values().get(property);
            long key = joinOn == JoinOn.SUBJECT ? item.subject() : item.object();
            long hashedKey = Hasher.hash(key);
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
     * @param partitions     partitions from the build phase
     * @param R              R relation table for the reference
     * @param S              S relation table to join
     * @param joinPropertyR  name of the property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  name of the property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               new joined table
     */
    @Override
    public ComplexTable probe(BuildOutput partitions, ComplexTable R, ComplexTable S,
                              String joinPropertyR, JoinOn joinOnR,
                              String joinPropertyS, JoinOn joinOnS) {
        HashMap<Long, List<JoinedItems>> hashedReferenceTablePartitions = ((HashJoinBuildOutput) partitions).getPartition();

        Dictionary referenceTableDictionary = R.getDictionary();
        Dictionary probeTableDictionary = S.getDictionary();
        List<JoinedItems> joinedItems = new ArrayList<>(); // Output list

        // For each tuple in S...
        S.getValues().forEach(probeItems -> {
            long hashedSKey;
            if (joinOnS == JoinOn.SUBJECT) {
                hashedSKey = Hasher.hash(probeItems.subject());
            } else{
                hashedSKey = Hasher.hash(probeItems.values().get(joinPropertyS).object());
            }

            // ... look up its join key in the hash table of R
            if (hashedReferenceTablePartitions.containsKey(hashedSKey)) {
                // A match is found. Output the combined tuple.
                for (JoinedItems matchingReferenceTableItems : hashedReferenceTablePartitions.get(hashedSKey)) {
                    if (!matchingReferenceTableItems.values().containsKey(joinPropertyR)) {
                        return; // Discard this S tuple
                    }
                    if (!probeItems.values().containsKey(joinPropertyS)) {
                        return; // Discard this S tuple
                    }

                    // Get join property
                    Item<Integer> referenceItem = matchingReferenceTableItems.values().get(joinPropertyR);
                    Item<Integer> probeItem = probeItems.values().get(joinPropertyS);

                    // Check if there is a hash collision
                    long referenceJoinKey = joinOnR == JoinOn.SUBJECT ? referenceItem.subject() : referenceItem.object();
                    long probeJoinKey = joinOnS == JoinOn.SUBJECT ? probeItem.subject() : probeItem.object();
                    if (referenceJoinKey == probeJoinKey) {
                        // No hash collision
                        mergeTuples(joinedItems, matchingReferenceTableItems, probeItems, referenceTableDictionary, probeTableDictionary);
                    }
                }
            }
        });

        // Concatenate properties of the two tables
        // Use a set to remove duplicates
        Set<String> set = new LinkedHashSet<>(R.getProperties());
        set.addAll(S.getProperties());

        Set<String> properties = new LinkedHashSet<>(set);

        return new ComplexTable(properties, referenceTableDictionary, new PropertyValues<>(joinedItems));
    }

}
