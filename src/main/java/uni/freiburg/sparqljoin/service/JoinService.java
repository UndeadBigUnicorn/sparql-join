package uni.freiburg.sparqljoin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uni.freiburg.sparqljoin.join.*;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.util.Performance;

@Service
public class JoinService {

    private static final Logger LOG = LoggerFactory.getLogger(JoinService.class);

    /**
     * Join 2 tables by given property using HashJoin algorithm
     * @param R              R relation join table
     * @param S              S relation join table
     * @param joinPropertyR  property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               joined table
     */
    public ComplexTable hashJoin(ComplexTable R, ComplexTable S,
                                 String joinPropertyR, JoinOn joinOnR,
                                 String joinPropertyS, JoinOn joinOnS) {
        int joinPropertyRInt = R.getPropertyDictionary().getInvertedValues().get(joinPropertyR);
        int joinPropertySInt = S.getPropertyDictionary().getInvertedValues().get(joinPropertyS);

        return join(new HashJoin(), R, S, joinPropertyRInt, joinOnR, joinPropertySInt, joinOnS);
    }

    /**
     * Join 2 tables by given property using parallel HashJoin algorithm
     * @param R              R relation join table
     * @param S              S relation join table
     * @param joinPropertyR  property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               joined table
     */
    public ComplexTable parallelHashJoin(ComplexTable R, ComplexTable S,
                                 String joinPropertyR, JoinOn joinOnR,
                                 String joinPropertyS, JoinOn joinOnS) {
        int joinPropertyRInt = R.getPropertyDictionary().getInvertedValues().get(joinPropertyR);
        int joinPropertySInt = S.getPropertyDictionary().getInvertedValues().get(joinPropertyS);

        return join(new ParallelHashJoin(), R, S, joinPropertyRInt, joinOnR, joinPropertySInt, joinOnS);
    }

    /**
     * Join 2 tables by given property using SortMergeJoin algorithm
     * @param R              R relation join table
     * @param S              S relation join table
     * @param joinPropertyR  property to join on from table R
     * @param joinOnR        join field in property from R
     * @param joinPropertyS  property to join on from table S
     * @param joinOnS        join field in property from S
     * @return               joined table
     */
    public ComplexTable sortMergeJoin(ComplexTable R, ComplexTable S,
                                 String joinPropertyR, JoinOn joinOnR,
                                 String joinPropertyS, JoinOn joinOnS) {
        int joinPropertyRInt = R.getPropertyDictionary().getInvertedValues().get(joinPropertyR);
        int joinPropertySInt = S.getPropertyDictionary().getInvertedValues().get(joinPropertyS);

        return join(new SortMergeJoin(), R, S, joinPropertyRInt, joinOnR, joinPropertySInt, joinOnS);
    }

    /**
     * Join call with performance measuring and details logging
     * @param joiner        Join implementation class
     * @param R             R relation join table
     * @param S             S relation join table
     * @param joinPropertyR join property from R
     * @param joinOnR       join field in property from R
     * @param joinPropertyS join property from S
     * @param joinOnS       join field in property from S
     * @return              joined table
     */
    private ComplexTable join(AbstractJoin joiner, ComplexTable R, ComplexTable S,
                              int joinPropertyR, JoinOn joinOnR,
                              int joinPropertyS, JoinOn joinOnS) {
        LOG.debug("Joining table '{}' on '{}'.{} = '{}'.{} ...",
                R.getPropertyDictionary(), R.getPropertyDictionary().get(joinPropertyR), joinOnR, R.getPropertyDictionary().get(joinPropertyS), joinOnS);
        ComplexTable joinedTable = Performance.measure(() ->
                joiner.join(R, S, joinPropertyR, joinOnR, joinPropertyS,  joinOnS), String.format("%s", joiner.getClass().getSimpleName())
        );
        LOG.debug("Table 1 length: {}", R.getValues().size());
        LOG.debug("Table 2 length: {}", S.getValues().size());
        LOG.debug("Joined table length: {}", joinedTable.getValues().size());
        return joinedTable;
    }
}
