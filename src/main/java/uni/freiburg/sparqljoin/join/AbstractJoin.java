package uni.freiburg.sparqljoin.join;

import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.SimpleTable;
import uni.freiburg.sparqljoin.model.join.BuildOutput;

/**
 * This interface provides functionality for joins
 */
public interface AbstractJoin {

    /**
     * Join 2 table
     * @param t1     first table
     * @param t2     second table
     * @param joinOn property value to join on name of the property to join on from the reference table
     * @return       joined table result
     */
    default ComplexTable join(ComplexTable t1, SimpleTable t2, String joinOn) {
        BuildOutput output = build(t1, joinOn);
        return probe(output, t1, t2, joinOn);
    }

    /**
     * Build phase of join
     * @param table  build input
     * @param joinOn property value to join on
     * @returns      build output
     */
    BuildOutput build(ComplexTable table, String joinOn);

    /**
     * Probe phase of join
     * @param partition      partition from the build phase
     * @param referenceTable first table for the reference
     * @param probeTable     to join
     * @param joinOn         name of the property to join on from the reference table
     * @returns              joined values
     */
    ComplexTable probe(BuildOutput partition, ComplexTable referenceTable, SimpleTable probeTable, String joinOn);
}
