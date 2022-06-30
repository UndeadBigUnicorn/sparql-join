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
                             String joinPropertyR, JoinOn joinOnR,
                             String joinPropertyS, JoinOn joinOnS) {
        MergeJoinBuildOutput sortedR = (MergeJoinBuildOutput) build(R, joinPropertyR, joinOnR);
        MergeJoinBuildOutput sortedS = (MergeJoinBuildOutput) build(S, joinPropertyS, joinOnS);
        MergeJoinBuildOutput buildOutput = new MergeJoinBuildOutput(sortedR.getValuesR(), sortedS.getValuesR());
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
    public BuildOutput build(ComplexTable table, String property, JoinOn joinOn) {
        List<JoinedItems> values = table.getValues();
        values.sort(new JoinedItems.JoinedItemsComparator(property, joinOn));
        return new MergeJoinBuildOutput(values);
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
                              String joinPropertyR, JoinOn joinOnR,
                              String joinPropertyS, JoinOn joinOnS) {
        MergeJoinBuildOutput buildOutput = (MergeJoinBuildOutput) partition;
        List<JoinedItems> referenceValues = buildOutput.getValuesR();
        List<JoinedItems> probeValues = buildOutput.getValuesS();

        // create new dictionary for merge
        Dictionary referenceTableDictionary = R.getDictionary();
        Dictionary probeTableDictionary = S.getDictionary();
        List<JoinedItems> joinedItems = new ArrayList<>(); // Output list

        int referenceRelIndex = 0;
        int probeRelIndex = 0;

        while(referenceRelIndex < referenceValues.size() && probeRelIndex < probeValues.size()) {
            JoinedItems referenceItems = referenceValues.get(referenceRelIndex);
            Item<Integer> referenceItem = referenceItems.values().get(joinPropertyR);
            JoinedItems probeItems = probeValues.get(probeRelIndex);
            Item<Integer> probeItem = probeItems.values().get(joinPropertyS);

            long referenceJoinKey = joinOnR == JoinOn.SUBJECT ? referenceItem.subject() : referenceItem.object();
            long probeJoinKey = joinOnS == JoinOn.SUBJECT ? probeItem.subject() : probeItem.object();

            // Forward pointers so that until a match is found
            if (referenceJoinKey > probeJoinKey) {
                probeRelIndex++;
            }
            else if (referenceJoinKey < probeJoinKey){
                referenceRelIndex++;
            } else {
                // Match is found

                mergeTuples(joinedItems, referenceItems, probeItems, referenceTableDictionary, probeTableDictionary);

                // output further tuples that match with reference item
                int probeRelIndexPrime = probeRelIndex + 1;
                while (probeRelIndexPrime < probeValues.size()) {
                    JoinedItems probeItemsNext = probeValues.get(probeRelIndexPrime);
                    Item<Integer> probeItemNext = probeItemsNext.values().get(joinPropertyS);
                    long probeJoinKeyNext = joinOnS == JoinOn.SUBJECT ? probeItemNext.subject() : probeItemNext.object();
                    if (referenceJoinKey == probeJoinKeyNext) {
                        mergeTuples(joinedItems, referenceItems, probeItemsNext, referenceTableDictionary, probeTableDictionary);
                        probeRelIndexPrime++;
                    } else {
                        break;
                    }
                }

                // output further tuples that match with probe item
                int referenceRelIndexPrime = referenceRelIndex+1;
                while (referenceRelIndexPrime < referenceValues.size()) {
                    JoinedItems referenceItemsNext = referenceValues.get(referenceRelIndexPrime);
                    Item<Integer> referenceItemNext = referenceItemsNext.values().get(joinPropertyR);
                    long referenceJoinKeyNext = joinOnR == JoinOn.SUBJECT ? referenceItemNext.subject() : referenceItemNext.object();
                    if (referenceJoinKeyNext == probeJoinKey) {
                        mergeTuples(joinedItems, referenceItemsNext, probeItems, referenceTableDictionary, probeTableDictionary);
                        referenceRelIndexPrime++;
                    } else {
                        break;
                    }
                }
                referenceRelIndex++;
                probeRelIndex++;
            }
        }

        // Concatenate properties of the two tables
        // Use a set to remove duplicates
        Set<String> set = new LinkedHashSet<>(R.getProperties());
        set.addAll(S.getProperties());

        Set<String> properties = new LinkedHashSet<>(set);

        return new ComplexTable(properties, referenceTableDictionary, new PropertyValues<>(joinedItems));
    }

}
