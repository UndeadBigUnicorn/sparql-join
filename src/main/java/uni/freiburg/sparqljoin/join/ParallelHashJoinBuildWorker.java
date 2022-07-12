package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.VerticallyPartitionedTable;
import uni.freiburg.sparqljoin.model.join.HashJoinBuildOutput;
import uni.freiburg.sparqljoin.model.join.HashJoinBuildOutputOptimized;

import java.util.concurrent.Callable;

public class ParallelHashJoinBuildWorker implements Callable<HashJoinBuildOutputOptimized> {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHashJoinBuildWorker.class);

    private final ParallelHashJoinOptimized join;
    private final VerticallyPartitionedTable buildRelationPart;

    private final String joinProperty;

    private final JoinOn joinOn;

    private HashJoinBuildOutputOptimized output = null;

    public ParallelHashJoinBuildWorker(ParallelHashJoinOptimized join, VerticallyPartitionedTable buildRelationPart,
                                       String joinProperty, JoinOn joinOn) {
        this.join = join;
        this.buildRelationPart = buildRelationPart;
        this.joinProperty = joinProperty;
        this.joinOn = joinOn;
    }

    public HashJoinBuildOutputOptimized call() {
        LOG.info("Starting...");

        this.output = join.build(buildRelationPart, joinProperty, joinOn);

        LOG.info("Finished");

        return this.output;
    }

    public HashJoinBuildOutputOptimized getOutput() {
        return output;
    }
}
