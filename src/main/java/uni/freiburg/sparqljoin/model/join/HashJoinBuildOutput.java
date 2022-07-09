package uni.freiburg.sparqljoin.model.join;

import lombok.val;

import java.util.HashMap;
import java.util.List;

/**
 * This class provides the output of the HashJoin build phase - Hash Table with partitions
 */
public class HashJoinBuildOutput extends BuildOutput {

    private final HashMap<Integer, List<JoinedItems>> partition;

    public HashJoinBuildOutput(HashMap<Integer, List<JoinedItems>> partition) {
        this.partition = partition;
    }

    public HashMap<Integer, List<JoinedItems>> getPartition() {
        return partition;
    }

    public void mergeFrom(HashJoinBuildOutput other) {
        other.getPartition().forEach((key, joinedItems) -> {
            List<JoinedItems> matchingJoinedItems = this.partition.get(key);
            if (matchingJoinedItems != null) {
                matchingJoinedItems.addAll(joinedItems);
            } else {
                this.partition.put(key, joinedItems);
            }
        });
    }
}
