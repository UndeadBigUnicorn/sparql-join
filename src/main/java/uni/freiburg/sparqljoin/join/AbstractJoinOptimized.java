package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.join.BuildOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This interface provides optimized functionality for joins
 */
public interface AbstractJoinOptimized {

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
    default VerticallyPartitionedTable join(VerticallyPartitionedTable R, VerticallyPartitionedTable S,
                                            String joinPropertyR, JoinOn joinOnR,
                                            String joinPropertyS, JoinOn joinOnS) {
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
    BuildOutput build(VerticallyPartitionedTable table, String property, JoinOn joinOn);

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
    VerticallyPartitionedTable probe(BuildOutput partition, VerticallyPartitionedTable R, VerticallyPartitionedTable S,
                                     String joinPropertyR, JoinOn joinOnR,
                                     String joinPropertyS, JoinOn joinOnS);


    /**
     * Merge 2 tuples into a single with all properties.
     *
     * @param output             to store merged tuples
     * @param referenceItemIndex index of the matched item in the R relation
     * @param R                  R relation table for the reference
     * @param probeItemIndex     index of the matched item in the S relation
     * @param S                  S relation table for the reference
     */
    default void mergeTuples(HashMap<String, PropertyValues<Item>> output,
                             int referenceItemIndex, VerticallyPartitionedTable R,
                             int probeItemIndex, VerticallyPartitionedTable S) {
        R.propertyItems().forEach((property, values) -> {
            List<Item> items = values.getValues();
            Item referenceItem = items.get(referenceItemIndex);
            PropertyValues<Item> outputValues = output.get(property);
            if (outputValues == null) {
                output.put(property, new PropertyValues<>(new ArrayList<>(List.of(referenceItem))));
            } else {
                outputValues.getValues().add(referenceItem);
            }
        });

        S.propertyItems().forEach((property, values) -> {
            List<Item> items = values.getValues();
            Item probeItem = items.get(probeItemIndex);
            PropertyValues<Item> outputValues = output.get(property);
            if (outputValues == null) {
                output.put(property, new PropertyValues<>(new ArrayList<>(List.of(probeItem))));
            } else {
                outputValues.getValues().add(probeItem);
            }
        });
    }
}

