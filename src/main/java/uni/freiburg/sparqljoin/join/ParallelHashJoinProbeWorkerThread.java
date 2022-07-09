package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.join.BuildOutput;

public class ParallelHashJoinProbeWorkerThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHashJoinProbeWorkerThread.class);

    private final ParallelHashJoin join;

    private final BuildOutput buildOutput;

    private final ComplexTable R;

    private final ComplexTable S;

    private final String joinPropertyR;

    private final JoinOn joinOnR;

    private final String joinPropertyS;

    private final JoinOn joinOnS;

    private ComplexTable output = null;

    public ParallelHashJoinProbeWorkerThread(ParallelHashJoin join, BuildOutput buildOutput, ComplexTable R, ComplexTable S, String joinPropertyR, JoinOn joinOnR, String joinPropertyS, JoinOn joinOnS) {
        this.join = join;
        this.buildOutput = buildOutput;
        this.R = R;
        this.S = S;
        this.joinPropertyR = joinPropertyR;
        this.joinOnR = joinOnR;
        this.joinPropertyS = joinPropertyS;
        this.joinOnS = joinOnS;
    }

    public void run() {
        LOG.info("Starting...");

        this.output = join.probe(buildOutput, R, S, joinPropertyR, joinOnR, joinPropertyS, joinOnS);

        LOG.info("Finished");
    }

    public ComplexTable getOutput() {
        return output;
    }
}
