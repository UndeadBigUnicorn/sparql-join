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
     * Join 2 tables
     */
    public static void join(SimpleTable t1, SimpleTable t2, String joinOn) {
        LOG.info("Joining tables {} -> {} on {}...", t1.getProperty(), t2.getProperty(), joinOn);
        ComplexTable joinedTable = Performance.measure(() ->
                new HashJoin().join(t1.toComplex(), t2, joinOn), "Hash Join"
        );
        LOG.info("Table 1 length: {}", t1.list().size());
        LOG.info("Table 2 length: {}", t2.list().size());
        LOG.info("Joined table length: {}", joinedTable.list().size());
    }
}
