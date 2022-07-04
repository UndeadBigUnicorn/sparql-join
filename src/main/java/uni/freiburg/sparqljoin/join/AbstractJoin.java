package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.ComplexTable;
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
     * Join 2 table
     * @param R              R relation join table
     * @param S              S relation join table
     * @param joinPropertyR  name of the property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  name of the property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               new joined table
     */
    default ComplexTable join(ComplexTable R, ComplexTable S,
                              String joinPropertyR, JoinOn joinOnR,
                              String joinPropertyS, JoinOn joinOnS) {
        BuildOutput output = build(R, joinPropertyR, joinOnR);
        return probe(output, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);
    }

    /**
     * Build phase of join
     * @param table    build input
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @returns        build output
     */
    BuildOutput build(ComplexTable table, String property, JoinOn joinOn);

    /**
     * Probe phase of join
     * @param partition      partition from the build phase
     * @param R              R relation table for the reference
     * @param S              S relation table to join
     * @param joinPropertyR  name of the property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  name of the property to join on from table S
     * @param joinOnS        join field in property from S
     * @returns              joined values in new table
     */
    ComplexTable probe(BuildOutput partition, ComplexTable R, ComplexTable S,
                       String joinPropertyR, JoinOn joinOnR,
                       String joinPropertyS, JoinOn joinOnS);


    /**
     * Merge 2 tuples into a single with all properties. Merge the dictionaries.
     *
     * @param joinedItems      Collection of joined items to add new tuple
     * @param outputDictionary Dictionary for the ouput relation
     * @param itemsR           items from R relation
     * @param itemsS           items from S relation
     * @param dictionaryR      R table dictionary
     * @param dictionaryS      S table dictionary
     */
    default void mergeTuplesAndDictionaries(List<JoinedItems> joinedItems, Dictionary outputDictionary, JoinedItems itemsR, JoinedItems itemsS, Dictionary dictionaryR, Dictionary dictionaryS) {
        // clone reference item values to avoid overwriting values by reference
        HashMap<String, Item<Integer>> values = (HashMap<String, Item<Integer>>) itemsR.values().clone();
        // add new property values
        itemsS.values().forEach((property, propertyItem) -> {
            // object was a string -> put value into new dictionary, update item value index
            if (dictionaryS.containsKey(propertyItem.object())) {
                // TODO the object might have also been a number - to do this reliably, we need a boolean variable for each subject/object that indicates whether a dictionary reference is meant or not. Alternatively, we could also insert integers into the dictionary as values.
                // TODO write a test for this
                values.put(property, new Item<>(
                        propertyItem.subject(),
                        (int) outputDictionary.put(dictionaryS.get(propertyItem.object()))
                ));
            } else {
                // else put as it is
                values.put(property, new Item<>(
                        propertyItem.subject(),
                        propertyItem.object())
                );
            }
        });
        joinedItems.add(new JoinedItems(itemsR.subject(), values));
    }
}
