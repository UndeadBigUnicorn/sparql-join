package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.db.Item;
import uni.freiburg.sparqljoin.model.db.PropertyValues;
import uni.freiburg.sparqljoin.model.join.BuildOutput;
import uni.freiburg.sparqljoin.model.join.JoinedItems;
import uni.freiburg.sparqljoin.model.join.MergeJoinBuildOutput;

import java.util.*;

/**
 * This class implements Sort-Merge join algorithm
 */
public class SortMergeJoin implements AbstractJoin {

    /**
     * Sort the two tables by the join attribute and merge by join condition
     * @param t1             first table
     * @param t2             second table
     * @param joinPropertyT1 name of the property to join on from the T1
     * @param joinOnT1       field from the t1 to join on
     * @param joinPropertyT2 name of the property to join on from the T2
     * @param joinOnT2       field from the t2 to join on
     * @return
     */
    @Override
    public ComplexTable join(ComplexTable t1, ComplexTable t2, String joinPropertyT1, String joinOnT1, String joinPropertyT2, String joinOnT2) {
        MergeJoinBuildOutput sortedT1 = (MergeJoinBuildOutput) build(t1, joinPropertyT1, joinOnT1);
        MergeJoinBuildOutput sortedT2 = (MergeJoinBuildOutput) build(t2, joinPropertyT2, joinOnT2);
        MergeJoinBuildOutput buildOutput = new MergeJoinBuildOutput();
        buildOutput.setValuesT1(sortedT1.getValuesT1());
        buildOutput.setValuesT2(sortedT2.getValuesT1());
        return probe(buildOutput, t1, t2, joinPropertyT1, joinOnT1, joinPropertyT2, joinOnT2);
    }

    /**
     * Sort table values by join attributes
     * @param table    build input
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @return         sorted table values
     */
    @Override
    public BuildOutput build(ComplexTable table, String property, String joinOn) {
        List<JoinedItems> values = table.getValues();
        values.sort(new JoinedItems.JoinedItemsComparator(property, joinOn));
        MergeJoinBuildOutput buildOutput = new MergeJoinBuildOutput();
        buildOutput.setValuesT1(values);
        return buildOutput;
    }

    /**
     * Merge sorted lists by join condition
     * @param partition      partition from the build phase
     * @param referenceTable first table for the reference
     * @param probeTable     to join
     * @param joinPropertyT1 name of the property to join on from the T1
     * @param joinOnT1       field from the t1 to join on
     * @param joinPropertyT2 name of the property to join on from the T2
     * @param joinOnT2       field from the t2 to join on
     * @return               joined values
     */
    @Override
    public ComplexTable probe(BuildOutput partition, ComplexTable referenceTable, ComplexTable probeTable, String joinPropertyT1, String joinOnT1, String joinPropertyT2, String joinOnT2) {
        MergeJoinBuildOutput buildOutput = (MergeJoinBuildOutput) partition;
        List<JoinedItems> referenceValues = buildOutput.getValuesT1();
        List<JoinedItems> probeValues = buildOutput.getValuesT2();

        // create new dictionary for merge
        Dictionary referenceTableDictionary = referenceTable.getDictionary();
        Dictionary probeTableDictionary = probeTable.getDictionary();
        List<JoinedItems> joinedItems = new ArrayList<>();

        int i = 0;
        int j = 0;

        while(i < referenceValues.size() && j < probeValues.size()) {
            JoinedItems referenceItems = referenceValues.get(i);
            Item<Integer> referenceItem = referenceItems.values().get(joinPropertyT1);
            JoinedItems probeItems = probeValues.get(j);
            Item<Integer> probeItem = probeItems.values().get(joinPropertyT2);

            long propertyJoinKey = joinOnT1.equals("subject") ? referenceItem.subject() : referenceItem.object();
            long probeJoinKey = joinOnT2.equals("subject") ? probeItem.subject() : probeItem.object();
            if (propertyJoinKey == probeJoinKey) {
                // clone reference item values to avoid overwriting values by reference
                HashMap<String, Item<Integer>> values = (HashMap<String, Item<Integer>>) referenceItems.values().clone();
                // add new property values
                probeItems.values().forEach((property, propertyItem) -> {
                    // object was a string -> put value into new dictionary, update item value index
                    if (probeTableDictionary.containsKey(propertyItem.object())) {
                        values.put(property, new Item<>(
                                propertyItem.subject(),
                                (int) referenceTableDictionary.put(probeTableDictionary.get(propertyItem.object()))
                        ));
                    } else {
                        // else put as it is
                        values.put(property, new Item<>(
                                propertyItem.subject(),
                                propertyItem.object())
                        );
                    }
                });
                joinedItems.add(new JoinedItems(referenceItems.subject(), values));
                j++;
            } else {
                i++;
            }
        }

        // concat properties of 2 tables
        Set<String> set = new LinkedHashSet<>(referenceTable.getProperties());
        set.addAll(probeTable.getProperties());
        List<String> properties = new ArrayList<>(set);
        return new ComplexTable(properties, referenceTableDictionary, new PropertyValues<>(joinedItems));
    }

}
