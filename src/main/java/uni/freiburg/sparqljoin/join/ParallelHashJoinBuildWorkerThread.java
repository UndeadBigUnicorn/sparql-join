package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.join.HashJoinBuildOutput;

public class ParallelHashJoinBuildWorkerThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHashJoinBuildWorkerThread.class);

    private final ParallelHashJoin join;
    private final ComplexTable buildRelationPart;

    private final int joinProperty;

    private final JoinOn joinOn;

    private HashJoinBuildOutput output = null;

    public ParallelHashJoinBuildWorkerThread(ParallelHashJoin join, ComplexTable buildRelationPart, int joinProperty, JoinOn joinOn) {
        this.join = join;
        this.buildRelationPart = buildRelationPart;
        this.joinProperty = joinProperty;
        this.joinOn = joinOn;
    }

    public void run() {
        LOG.info("Starting...");

        this.output = join.build(buildRelationPart, joinProperty, joinOn);

        LOG.info("Finished");
    }

    public HashJoinBuildOutput getOutput() {
        return output;
    }
}
