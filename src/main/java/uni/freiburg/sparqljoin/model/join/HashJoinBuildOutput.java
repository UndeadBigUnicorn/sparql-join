package uni.freiburg.sparqljoin.model.join;

import lombok.val;

import java.util.HashMap;
import java.util.List;

/**
 * This class provides the output of the HashJoin build phase - Hash Table with partitions
 */
public class HashJoinBuildOutput extends BuildOutput {

    private final HashMap<Long, List<JoinedItems>> partition;

    public HashJoinBuildOutput(HashMap<Long, List<JoinedItems>> partition) {
        this.partition = partition;
    }

    public HashMap<Long, List<JoinedItems>> getPartition() {
        return partition;
    }

    public void mergeFrom(HashJoinBuildOutput other) {
        other.getPartition().forEach((key, joinedItems) -> {
            val matchingJoinedItems = this.partition.get(key);
            if (matchingJoinedItems != null) {
                matchingJoinedItems.addAll(joinedItems);
            } else {
                this.partition.put(key, joinedItems);
            }
        });
    }
}
