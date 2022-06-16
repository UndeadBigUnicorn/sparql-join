package uni.freiburg.sparqljoin.model.join;

import java.util.List;

/**
 * This class provides a shared interface for outputting sorted values
 * from the build phase into probe phase of SortMerge join algorithm
 */
public class MergeJoinBuildOutput extends BuildOutput{

    private List<JoinedItems> valuesR;

    private List<JoinedItems> valuesS;

    public MergeJoinBuildOutput(List<JoinedItems> valuesR) {
        this.valuesR = valuesR;
    }

    public MergeJoinBuildOutput(List<JoinedItems> valuesR, List<JoinedItems> valuesS) {
        this.valuesR = valuesR;
        this.valuesS = valuesS;
    }

    public List<JoinedItems> getValuesR() {
        return valuesR;
    }

    public List<JoinedItems> getValuesS() {
        return valuesS;
    }
}
