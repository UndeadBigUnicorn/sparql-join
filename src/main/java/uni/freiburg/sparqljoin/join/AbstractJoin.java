package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.join.BuildOutput;

/**
 * This interface provides functionality for joins
 */
public interface AbstractJoin {

    /**
     * Join 2 table
     * @param t1             first table
     * @param t2             second table
     * @param joinPropertyT1 name of the property to join on from the T1
     * @param joinOnT1       field from the t1 to join on
     * @param joinPropertyT2 name of the property to join on from the T2
     * @param joinOnT2       field from the t2 to join on
     * @return               joined table result
     */
    default ComplexTable join(ComplexTable t1, ComplexTable t2, String joinPropertyT1, String joinOnT1, String joinPropertyT2, String joinOnT2) {
        BuildOutput output = build(t1, joinPropertyT1, joinOnT1);
        return probe(output, t1, t2, joinPropertyT1, joinOnT1, joinPropertyT2, joinOnT2);
    }

    /**
     * Build phase of join
     * @param table    build input
     * @param property property value to join on name of the property to join on from the reference table
     * @param joinOn   property field to join on
     * @returns        build output
     */
    BuildOutput build(ComplexTable table, String property, String joinOn);

    /**
     * Probe phase of join
     * @param partition      partition from the build phase
     * @param referenceTable first table for the reference
     * @param probeTable     to join
     * @param joinPropertyT1 name of the property to join on from the T1
     * @param joinOnT1       field from the t1 to join on
     * @param joinPropertyT2 name of the property to join on from the T2
     * @param joinOnT2       field from the t2 to join on
     * @returns              joined values
     */
    ComplexTable probe(BuildOutput partition, ComplexTable referenceTable, ComplexTable probeTable,
                       String joinPropertyT1, String joinOnT1,
                       String joinPropertyT2, String joinOnT2);
}
