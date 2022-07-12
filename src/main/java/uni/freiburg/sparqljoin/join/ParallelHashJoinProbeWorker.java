package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.VerticallyPartitionedTable;
import uni.freiburg.sparqljoin.model.join.BuildOutput;

import java.util.concurrent.Callable;

public class ParallelHashJoinProbeWorker implements Callable<VerticallyPartitionedTable> {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHashJoinProbeWorker.class);

    private final ParallelHashJoinOptimized join;

    private final BuildOutput buildOutput;

    private final VerticallyPartitionedTable R;

    private final VerticallyPartitionedTable S;

    private final String joinPropertyR;

    private final JoinOn joinOnR;

    private final String joinPropertyS;

    private final JoinOn joinOnS;

    public ParallelHashJoinProbeWorker(ParallelHashJoinOptimized join, BuildOutput buildOutput,
                                       VerticallyPartitionedTable R, VerticallyPartitionedTable S,
                                       String joinPropertyR, JoinOn joinOnR,
                                       String joinPropertyS, JoinOn joinOnS) {
        this.join = join;
        this.buildOutput = buildOutput;
        this.R = R;
        this.S = S;
        this.joinPropertyR = joinPropertyR;
        this.joinOnR = joinOnR;
        this.joinPropertyS = joinPropertyS;
        this.joinOnS = joinOnS;
    }

    public VerticallyPartitionedTable call() {
        LOG.info("Starting...");

        VerticallyPartitionedTable output = join.probe(buildOutput, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);

        LOG.info("Finished");

        return output;
    }

}
