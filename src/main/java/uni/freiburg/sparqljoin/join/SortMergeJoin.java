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
     * Sort two tables by the join attribute and merge by join condition
     * @param R              R relation join table
     * @param S              S relation join table
     * @param joinPropertyR  name of the property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  name of the property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               new joined table
     */
    @Override
    public ComplexTable join(ComplexTable R, ComplexTable S,
                             String joinPropertyR, String joinOnR,
                             String joinPropertyS, String joinOnS) {
        MergeJoinBuildOutput sortedR = (MergeJoinBuildOutput) build(R, joinPropertyR, joinOnR);
        MergeJoinBuildOutput sortedS = (MergeJoinBuildOutput) build(S, joinPropertyS, joinOnS);
        MergeJoinBuildOutput buildOutput = new MergeJoinBuildOutput();
        buildOutput.setValuesR(sortedR.getValuesR());
        buildOutput.setValuesS(sortedS.getValuesR());
        return probe(buildOutput, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);
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
        buildOutput.setValuesR(values);
        return buildOutput;
    }

    /**
     * Merge sorted lists by join condition
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
        MergeJoinBuildOutput buildOutput = (MergeJoinBuildOutput) partition;
        List<JoinedItems> referenceValues = buildOutput.getValuesR();
        List<JoinedItems> probeValues = buildOutput.getValuesS();

        // create new dictionary for merge
        Dictionary referenceTableDictionary = R.getDictionary();
        Dictionary probeTableDictionary = S.getDictionary();
        List<JoinedItems> joinedItems = new ArrayList<>();

        int i = 0;
        int j = 0;

        while(i < referenceValues.size() && j < probeValues.size()) {
            JoinedItems referenceItems = referenceValues.get(i);
            Item<Integer> referenceItem = referenceItems.values().get(joinPropertyR);
            JoinedItems probeItems = probeValues.get(j);
            Item<Integer> probeItem = probeItems.values().get(joinPropertyS);

            long propertyJoinKey = joinOnR.equals("subject") ? referenceItem.subject() : referenceItem.object();
            long probeJoinKey = joinOnS.equals("subject") ? probeItem.subject() : probeItem.object();
            if (propertyJoinKey > probeJoinKey) {
                j++;
            }
            else if (propertyJoinKey < probeJoinKey){
                i++;
            } else {
                mergeTuples(joinedItems, referenceItems, probeItems, referenceTableDictionary, probeTableDictionary);

                // output further tuples that match with reference item
                int jPrime = j+1;
                while (jPrime < probeValues.size()) {
                    JoinedItems probeItemsNext = probeValues.get(jPrime);
                    Item<Integer> probeItemNext = probeItemsNext.values().get(joinPropertyS);
                    long probeJoinKeyNext = joinOnS.equals("subject") ? probeItemNext.subject() : probeItemNext.object();
                    if (propertyJoinKey == probeJoinKeyNext) {
                        mergeTuples(joinedItems, referenceItems, probeItemsNext, referenceTableDictionary, probeTableDictionary);
                        jPrime++;
                    } else {
                        break;
                    }
                }

                // output further tuples that match with probe item
                int iPrime = i+1;
                while (iPrime < referenceValues.size()) {
                    JoinedItems referenceItemsNext = referenceValues.get(iPrime);
                    Item<Integer> referenceItemNext = referenceItemsNext.values().get(joinPropertyR);
                    long referenceJoinKeyNext = joinOnR.equals("subject") ? referenceItemNext.subject() : referenceItemNext.object();
                    if (referenceJoinKeyNext == probeJoinKey) {
                        mergeTuples(joinedItems, referenceItemsNext, probeItems, referenceTableDictionary, probeTableDictionary);
                        iPrime++;
                    } else {
                        break;
                    }
                }
                i++;
                j++;
            }
        }

        // concat properties of 2 tables
        Set<String> set = new LinkedHashSet<>(R.getProperties());
        set.addAll(S.getProperties());
        List<String> properties = new ArrayList<>(set);
        return new ComplexTable(properties, referenceTableDictionary, new PropertyValues<>(joinedItems));
    }

}
