package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.DataType;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.db.Item;
import uni.freiburg.sparqljoin.model.join.BuildOutput;
import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.HashMap;
import java.util.List;

/**
 * This interface provides functionality for joins
 */
public interface AbstractJoin {

    /**
     * Join 2 tables
     *
     * @param R             R relation join table
     * @param S             S relation join table
     * @param joinPropertyR name of the property to join on from table R
     * @param joinOnR       join field in property from R
     * @param joinPropertyS name of the property to join on from table S
     * @param joinOnS       join field in property from S
     * @return new joined table
     */
    default ComplexTable join(ComplexTable R, ComplexTable S,
                              int joinPropertyR, JoinOn joinOnR,
                              int joinPropertyS, JoinOn joinOnS) {
        BuildOutput buildOutput = build(R, joinPropertyR, joinOnR);
        return probe(buildOutput, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);
    }

    /**
     * Build phase of join
     *
     * @param table    build input
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @return build output
     */
    BuildOutput build(ComplexTable table, int property, JoinOn joinOn);

    /**
     * Probe phase of join
     *
     * @param partition     partition from the build phase
     * @param R             R relation table for the reference
     * @param S             S relation table to join
     * @param joinPropertyR name of the property to join on from table R
     * @param joinOnR       join field in property from R
     * @param joinPropertyS name of the property to join on from table S
     * @param joinOnS       join field in property from S
     * @return joined values in new table
     */
    ComplexTable probe(BuildOutput partition, ComplexTable R, ComplexTable S,
                       int joinPropertyR, JoinOn joinOnR,
                       int joinPropertyS, JoinOn joinOnS);


    /**
     * Merge 2 tuples into a single with all properties. Merge the dictionaries.
     *
     * @param joinedItems  Output relation where the new tuple should be added
     * @param joinedItemsR Items from R relation
     * @param joinedItemsS Items from S relation
     */
    default void mergeTuplesAndDictionaries(List<JoinedItems> joinedItems, JoinedItems joinedItemsR, JoinedItems joinedItemsS) {
        // clone reference item values to avoid overwriting values by reference
        @SuppressWarnings("unchecked") HashMap<Integer, Item> outputValues = (HashMap<Integer, Item>) joinedItemsR.values().clone();

        outputValues.putAll(joinedItemsS.values());
        joinedItems.add(new JoinedItems(joinedItemsR.subject(), outputValues));
    }
}
