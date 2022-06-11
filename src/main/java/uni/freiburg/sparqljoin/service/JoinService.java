package uni.freiburg.sparqljoin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uni.freiburg.sparqljoin.join.HashJoin;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.SimpleTable;
import uni.freiburg.sparqljoin.util.Performance;

@Service
public class JoinService {

    private static final Logger LOG = LoggerFactory.getLogger(JoinService.class);

    /**
     * Join 2 tables by given property
     * @param property to join on
     * @param joinOnT1 join field from table 1
     * @param joinOnT2 join field from table 2
     */
    public ComplexTable join(ComplexTable t1, SimpleTable t2, String property, String joinOnT1, String joinOnT2) {
        LOG.debug("Joining table '{}' on '{}'.{} = '{}'.{} ...",
                t1.getProperties(), property, joinOnT1, t2.getProperty(), joinOnT2);
        ComplexTable joinedTable = Performance.measure(() ->
                new HashJoin().join(t1, t2, property, joinOnT1, joinOnT2), "Hash Join"
        );
        LOG.debug("Table 1 length: {}", t1.list().size());
        LOG.debug("Table 2 length: {}", t2.list().size());
        LOG.debug("Joined table length: {}", joinedTable.list().size());
        return joinedTable;
    }
}