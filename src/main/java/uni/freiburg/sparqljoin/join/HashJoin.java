package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.join.*;
import uni.freiburg.sparqljoin.util.Hasher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

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
        table.list().forEach(properties -> {
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
     * @param referenceTable first table for the reference
     * @param probeTable     second table to join
     * @param property       name of the property to join on from the reference table
     * @param joinOnT1       field from the referenceTable to join on
     * @param joinOnT2       field from the probeTable to join on
     * @return               joined table with combined properties
     */
    @Override
    public ComplexTable probe(BuildOutput partition, ComplexTable referenceTable, SimpleTable probeTable, String property, String joinOnT1, String joinOnT2) {
        HashMap<Long, List<JoinedItems>> hashJoinPartition = ((HashJoinBuildOutput) partition).getPartition();
        // create new dictionary for merge
        Dictionary referenceTableDictionary = referenceTable.getDictionary();
        Dictionary probeTableDictionary = probeTable.getDictionary();
        Dictionary newDict = new Dictionary();
        List<JoinedItems> joinedItems = new ArrayList<>();
        probeTable.list().forEach(probeItem -> {
            long hashed = Hasher.hash(probeItem.subject());
            // hash over join key matches existing bucket:
            // check items in the bucket if the key matches exactly
            if (hashJoinPartition.containsKey(hashed)) {
                for (JoinedItems partitionedItems : hashJoinPartition.get(hashed)) {
                    // each item value contains pairs of subject - object
                    // get join property
                    if (!partitionedItems.values().containsKey(property)) {
                        return;
                    }
                    Item<Integer> referenceItem = partitionedItems.values().get(property);
                    // check if join key of the property value in the partition is equal to the join key of the prob item
                    long propertyJoinKey = joinOnT1.equals("subject") ? referenceItem.subject() : referenceItem.object();
                    long probeJoinKey = joinOnT2.equals("subject") ? probeItem.subject() : probeItem.object();
                    if (propertyJoinKey == probeJoinKey) {
                        // object was a string -> put value into new dictionary, update item value index
                        if (referenceTableDictionary.containsKey(referenceItem.object())) {
                            partitionedItems.values().put(property, new Item<>(
                                    referenceItem.subject(),
                                    (int) newDict.put(referenceTableDictionary.get(referenceItem.object()))
                            ));
                        }
                        if (probeTableDictionary.containsKey(probeItem.object())) {
                            probeItem = new Item<>(
                                    probeItem.subject(),
                                    (int) newDict.put(probeTableDictionary.get(probeItem.object()))
                            );
                        }
                        // add new property values
                        partitionedItems.values().put(probeTable.getProperty(), probeItem);
                        joinedItems.add(partitionedItems);
                    }
                }
            }
        });

        // concat properties of 2 tables
        List<String> properties = Stream.concat(referenceTable.getProperties().stream(), Stream.of(probeTable.getProperty())).toList();
        return new ComplexTable(properties, newDict, new PropertyValues<>(joinedItems));
    }

}
