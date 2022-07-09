package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.DataType;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.db.Item;
import uni.freiburg.sparqljoin.model.join.BuildOutput;
import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.HashMap;
import java.util.LinkedHashSet;
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
                              String joinPropertyR, JoinOn joinOnR,
                              String joinPropertyS, JoinOn joinOnS) {
        BuildOutput buildOutput = build(R, joinPropertyR, joinOnR);
        ComplexTable probeOutput = probe(buildOutput, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);


        // Remove unnecessary dictionary entries
        ComplexTable joinResult = new ComplexTable(new LinkedHashSet<>());
        joinResult.insertComplexTable(probeOutput);

        return joinResult;
    }

    /**
     * Build phase of join
     *
     * @param table    build input
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @return build output
     */
    BuildOutput build(ComplexTable table, String property, JoinOn joinOn);

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
                       String joinPropertyR, JoinOn joinOnR,
                       String joinPropertyS, JoinOn joinOnS);


    /**
     * Merge 2 tuples into a single with all properties. Merge the dictionaries.
     *
     * @param joinedItems      Output relation where the new tuple should be added
     * @param outputDictionary Dictionary of the output relation
     * @param joinedItemsR     Items from R relation
     * @param joinedItemsS     Items from S relation
     * @param dictionaryR      R table dictionary
     * @param dictionaryS      S table dictionary
     */
    default void mergeTuplesAndDictionaries(List<JoinedItems> joinedItems, Dictionary outputDictionary, JoinedItems joinedItemsR, JoinedItems joinedItemsS, Dictionary dictionaryR, Dictionary dictionaryS) {
        // clone reference item values to avoid overwriting values by reference
        @SuppressWarnings("unchecked") HashMap<String, Item<Integer>> outputValues = (HashMap<String, Item<Integer>>) joinedItemsR.values().clone();

        // add new property values
        joinedItemsS.values().forEach((property, propertyItem) -> {
            if (propertyItem.type().equals(DataType.STRING)) {
                // object was a string -> put value into new dictionary, update item value index
                // TODO write a test for this

                int propertyItemObjectIntRepresentation = (int) outputDictionary.put(dictionaryS.get(propertyItem.object()));

                outputValues.put(property, new Item<>(
                        propertyItem.subject(),
                        propertyItemObjectIntRepresentation,
                        propertyItem.type()
                ));
            } else {
                // else put as it is
                outputValues.put(property, new Item<>(
                        propertyItem.subject(),
                        propertyItem.object(),
                        propertyItem.type())
                );
            }
        });
        joinedItems.add(new JoinedItems(joinedItemsR.subject(), outputValues));
    }
}
