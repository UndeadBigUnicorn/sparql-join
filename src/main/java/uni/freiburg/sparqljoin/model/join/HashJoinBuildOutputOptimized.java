package uni.freiburg.sparqljoin.model.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides the output of the HashJoin build phase - Hash Table with partitions:
 * key: hashed join attribute - indexes of matching elements
 */
public class HashJoinBuildOutputOptimized extends BuildOutput {

    private final HashMap<Integer, List<Integer>> partition;

    public HashJoinBuildOutputOptimized(HashMap<Integer, List<Integer>> partition) {
        this.partition = partition;
    }

    public HashMap<Integer, List<Integer>> getPartition() {
        return partition;
    }

    public void mergeFrom(HashJoinBuildOutputOptimized other, int part, int numParts) {
        other.getPartition().forEach((key, integers) -> {
            // convert indices from relative in the partition to real indices in the whole relation
            List<Integer> matchingPartitionedItems = this.partition.get(key);
            if (matchingPartitionedItems != null) {
                for (int i: integers) {
                    matchingPartitionedItems.add(i*numParts + part);
                }
            } else {
                List<Integer> indices = new ArrayList<>(integers.size());
                for (int i: integers) {
                    indices.add(i*numParts + part);
                }
                this.partition.put(key, indices);
            }
        });
    }
}
