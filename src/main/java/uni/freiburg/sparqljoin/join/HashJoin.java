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
     * for each item in the table
     * take a hash function of the join key and put value to the hash table partition
     *
     * @param table    build input
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @return         build output - Hash Table
     */
    @Override
    public HashJoinBuildOutput build(ComplexTable table, String property, String joinOn) {
        HashMap<Long, List<JoinedItems>> buildOutput = new HashMap<>();
        table.getValues().forEach(properties -> {
            if (!properties.values().containsKey(property)) {
                return;
            }
            Item<Integer> item = properties.values().get(property);
            long key = joinOn.equals("subject") ? item.subject() : item.object();
            long hashed = Hasher.hash(key);
            if (buildOutput.containsKey(hashed)) {
                List<JoinedItems> list = buildOutput.get(hashed);
                list.add(properties);
                buildOutput.put(hashed, list);
            } else {
                buildOutput.put(hashed, new ArrayList<>(List.of(properties)));
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
     * @param partition      partition from the build phase
     * @param R              R relation table for the reference
     * @param S              S relation table to join
     * @param joinPropertyR  name of the property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  name of the property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               new joined table
     */
    @Override
    public ComplexTable probe(BuildOutput partition, ComplexTable R, ComplexTable S,
                              String joinPropertyR, String joinOnR,
                              String joinPropertyS, String joinOnS) {
        HashMap<Long, List<JoinedItems>> hashJoinPartition = ((HashJoinBuildOutput) partition).getPartition();
        // create new dictionary for merge
        Dictionary referenceTableDictionary = R.getDictionary();
        Dictionary probeTableDictionary = S.getDictionary();
        List<JoinedItems> joinedItems = new ArrayList<>();
        S.getValues().forEach(probeItems -> {
            long hashed = Hasher.hash(probeItems.subject());
            // hash over join key matches existing bucket:
            // check items in the bucket if the key matches exactly
            if (hashJoinPartition.containsKey(hashed)) {
                for (JoinedItems partitionedItems : hashJoinPartition.get(hashed)) {
                    // each item value contains pairs of subject - object
                    // get join property
                    if (!partitionedItems.values().containsKey(joinPropertyR)) {
                        return;
                    }
                    Item<Integer> referenceItem = partitionedItems.values().get(joinPropertyR);
                    if (!probeItems.values().containsKey(joinPropertyS)) {
                        return;
                    }
                    Item<Integer> probeItem = probeItems.values().get(joinPropertyS);
                    // check if join key of the property value in the partition is equal to the join key of the prob item
                    long referenceJoinKey = joinOnR.equals("subject") ? referenceItem.subject() : referenceItem.object();
                    long probeJoinKey = joinOnS.equals("subject") ? probeItem.subject() : probeItem.object();
                    if (referenceJoinKey == probeJoinKey) {
                        mergeTuples(joinedItems, partitionedItems, probeItems, referenceTableDictionary, probeTableDictionary);
                    }
                }
            }
        });

        // concat properties of 2 tables
        Set<String> set = new LinkedHashSet<>(R.getProperties());
        set.addAll(S.getProperties());
        List<String> properties = new ArrayList<>(set);
        return new ComplexTable(properties, referenceTableDictionary, new PropertyValues<>(joinedItems));
    }

}
