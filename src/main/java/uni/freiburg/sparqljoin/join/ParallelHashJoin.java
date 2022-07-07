package uni.freiburg.sparqljoin.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.join.HashJoinBuildOutput;
import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

public class ParallelHashJoin extends HashJoin {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHashJoin.class);

    private List<ComplexTable> splitRelationIntoParts(ComplexTable relation, int numParts) {
        List<ComplexTable> relationParts = new ArrayList<>();

        for (int i = 0; i < numParts; i++) {
            relationParts.add(new ComplexTable(new LinkedHashSet<>()));
        }

        int numProcessedElements = 0;
        for (JoinedItems items : relation.getValues()) {
            relationParts.get(numProcessedElements % numParts).insert(items, relation.getDictionary());
            numProcessedElements++;
        }

        return relationParts;
    }

    @Override
    public ComplexTable join(ComplexTable R, ComplexTable S, String joinPropertyR, JoinOn joinOnR, String joinPropertyS, JoinOn joinOnS) {
        // TODO clean code
        // TODO combine splitting R and S relations into one thread

        int numThreads = 6;

        // Partition relation R across all threads for the build phase
        List<ComplexTable> buildRelationParts = splitRelationIntoParts(R, numThreads);

        // Start build threads
        List<ParallelHashJoinBuildWorkerThread> buildThreads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            ParallelHashJoinBuildWorkerThread thread = new ParallelHashJoinBuildWorkerThread(this, buildRelationParts.get(i), joinPropertyR, joinOnR);
            buildThreads.add(thread);
            thread.start();
        }

        // Wait for build threads to finish and get results
        List<HashJoinBuildOutput> buildOutputs = new ArrayList<>();
        for (ParallelHashJoinBuildWorkerThread thread : buildThreads) {
            try {
                thread.join();
                buildOutputs.add(thread.getOutput());
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException occurred while joining thread: {}", e.toString());
            }
        }


        // Combine build thread results
        HashJoinBuildOutput combinedBuildOutput = new HashJoinBuildOutput(new HashMap<>());
        for (HashJoinBuildOutput threadOutput : buildOutputs) {
            combinedBuildOutput.mergeFrom(threadOutput);
        }

        // Partition relation S across all threads for the probe phase
        List<ComplexTable> probeRelationParts = splitRelationIntoParts(S, numThreads);

        // Start probe threads
        List<ParallelHashJoinProbeWorkerThread> probeThreads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            ParallelHashJoinProbeWorkerThread thread = new ParallelHashJoinProbeWorkerThread(this, combinedBuildOutput, R, probeRelationParts.get(i), joinPropertyR, joinOnR, joinPropertyS, joinOnS);
            probeThreads.add(thread);
            thread.start();
        }

        // Wait for probe threads to finish and get results
        List<ComplexTable> probeOutputs = new ArrayList<>();
        for (ParallelHashJoinProbeWorkerThread thread : probeThreads) {
            try {
                thread.join();
                probeOutputs.add(thread.getOutput());
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException occurred while joining thread: {}", e.toString());
            }
        }

        // Combine probe thread results
        ComplexTable joinResult = new ComplexTable(new LinkedHashSet<>());
        for (ComplexTable threadOutput : probeOutputs) {
            joinResult.insertComplexTable(threadOutput);
        }

        return joinResult;
    }
}
