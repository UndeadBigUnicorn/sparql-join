package uni.freiburg.sparqljoin.model.join;

import uni.freiburg.sparqljoin.model.db.Item;

import java.util.HashMap;
import java.util.List;

/**
 * This class provides the output of the HashJoin build phase - Hash Table with partitions
 * @param \<V> type of the object values in the partitions
 */
public class HashJoinBuildOutput extends BuildOutput{

    private final HashMap<Long, List<JoinedItems>> partition;

    public HashJoinBuildOutput(HashMap<Long, List<JoinedItems>> partition) {
        this.partition = partition;
    }

    public HashMap<Long, List<JoinedItems>> getPartition() {
        return partition;
    }
}
