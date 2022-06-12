package uni.freiburg.sparqljoin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uni.freiburg.sparqljoin.join.HashJoin;
import uni.freiburg.sparqljoin.join.SortMergeJoin;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.util.Performance;

@Service
public class JoinService {

    private static final Logger LOG = LoggerFactory.getLogger(JoinService.class);

    /**
     * Join 2 tables by given property using HashJoin algorithm
     * @param joinPropertyT1 property to join on from table 1
     * @param joinOnT1       join field from table 1
     * @param joinPropertyT2 property to join on from table 2
     * @param joinOnT2       join field from table 2
     */
    public ComplexTable hashJoin(ComplexTable t1, ComplexTable t2,
                                 String joinPropertyT1, String joinOnT1,
                                 String joinPropertyT2, String joinOnT2) {
        LOG.debug("Joining table '{}' on '{}'.{} = '{}'.{} ...",
                t1.getProperties(), joinPropertyT1, joinOnT1, joinPropertyT2, joinOnT2);
        ComplexTable joinedTable = Performance.measure(() ->
                new HashJoin().join(t1, t2, joinPropertyT1, joinOnT1, joinPropertyT2,  joinOnT2), "Hash Join"
        );
        LOG.debug("Table 1 length: {}", t1.getValues().size());
        LOG.debug("Table 2 length: {}", t2.getValues().size());
        LOG.debug("Joined table length: {}", joinedTable.getValues().size());
        return joinedTable;
    }

    /**
     * Join 2 tables by given property using SortMergeJoin algorithm
     * @param joinPropertyT1 property to join on from table 1
     * @param joinOnT1       join field from table 1
     * @param joinPropertyT2 property to join on from table 2
     * @param joinOnT2       join field from table 2
     */
    public ComplexTable sortMergeJoin(ComplexTable t1, ComplexTable t2,
                                 String joinPropertyT1, String joinOnT1,
                                 String joinPropertyT2, String joinOnT2) {
        LOG.debug("Joining table '{}' on '{}'.{} = '{}'.{} ...",
                t1.getProperties(), joinPropertyT1, joinOnT1, joinPropertyT2, joinOnT2);
        ComplexTable joinedTable = Performance.measure(() ->
                new SortMergeJoin().join(t1, t2, joinPropertyT1, joinOnT1, joinPropertyT2,  joinOnT2), "Sort Merge Join"
        );
        LOG.debug("Table 1 length: {}", t1.getValues().size());
        LOG.debug("Table 2 length: {}", t2.getValues().size());
        LOG.debug("Joined table length: {}", joinedTable.getValues().size());
        return joinedTable;
    }


}
