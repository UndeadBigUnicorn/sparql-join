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
     * @return build output - HashMap with key = hashed join key, value = list of JoinedItems
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
     * find matching build hash map partition
     * compare and join items in the matching bucket
     *
     * @param buildOutput       output of the build phase, contains hashed build relation partitions
     * @param buildRelation     build relation table to join
     * @param probeRelation     probe relation table to join
     * @param buildJoinProperty name of the property to join on from the build relation
     * @param buildJoinOn       join field in the property from the build relation
     * @param probeJoinProperty name of the property to join on from the probe relation
     * @param probeJoinOn       join field in the property from the probe relation
     * @return new joined table
     */
    @Override
    public ComplexTable probe(BuildOutput buildOutput, ComplexTable buildRelation, ComplexTable probeRelation,
                              String buildJoinProperty, JoinOn buildJoinOn,
                              String probeJoinProperty, JoinOn probeJoinOn) {
        HashMap<Long, List<JoinedItems>> hashedBuildTablePartitions = ((HashJoinBuildOutput) buildOutput).getPartition();

        Dictionary probeTableDictionary = probeRelation.getDictionary();
        Dictionary buildTableDictionary = buildRelation.getDictionary();
        List<JoinedItems> joinedItems = new ArrayList<>(); // Output list

        // For each tuple in probeRelation...
        probeRelation.getValues().forEach(probeItems -> {
            long hashedProbeKey;
            if (probeJoinOn == JoinOn.SUBJECT) {
                hashedProbeKey = Hasher.hash(probeItems.subject());
            } else {
                hashedProbeKey = Hasher.hash(probeItems.values().get(probeJoinProperty).object());
            }

            // ... look up its join key in the hash table of buildRelation
            if (hashedBuildTablePartitions.containsKey(hashedProbeKey)) {
                // A match is found. Output the combined tuple.
                for (JoinedItems matchingBuildTableItems : hashedBuildTablePartitions.get(hashedProbeKey)) {
                    if (!matchingBuildTableItems.values().containsKey(buildJoinProperty)) {
                        return; // Discard this buildRelation tuple
                    }
                    if (!probeItems.values().containsKey(probeJoinProperty)) {
                        return; // Discard this buildRelation tuple
                    }

                    // Get join property
                    Item<Integer> buildItem = matchingBuildTableItems.values().get(buildJoinProperty);
                    Item<Integer> probeItem = probeItems.values().get(probeJoinProperty);

                    // Check if there is a hash collision
                    long buildJoinKey = buildJoinOn == JoinOn.SUBJECT ? buildItem.subject() : buildItem.object();
                    long probeJoinKey = probeJoinOn == JoinOn.SUBJECT ? probeItem.subject() : probeItem.object();
                    if (buildJoinKey == probeJoinKey) {
                        // No hash collision
                        mergeTuplesAndDictionaries(joinedItems, matchingBuildTableItems, probeItems, probeTableDictionary, buildTableDictionary);
                    }
                }
            }
        });

        // Concatenate properties of the two tables
        // Use a set to remove duplicates
        Set<String> set = new LinkedHashSet<>(probeRelation.getProperties());
        set.addAll(buildRelation.getProperties());

        Set<String> properties = new LinkedHashSet<>(set);

        return new ComplexTable(properties, probeTableDictionary, new PropertyValues<>(joinedItems));
    }

}
