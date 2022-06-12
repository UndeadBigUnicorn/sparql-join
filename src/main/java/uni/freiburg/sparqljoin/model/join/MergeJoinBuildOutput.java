package uni.freiburg.sparqljoin.model.join;

import java.util.List;

/**
 * This class provides a shared interface for outputting sort values from the build phase
 */
public class MergeJoinBuildOutput extends BuildOutput{

    private List<JoinedItems> valuesT1;

    private List<JoinedItems> valuesT2;

    public MergeJoinBuildOutput() {}

    public void setValuesT1(List<JoinedItems> valuesT1) {
        this.valuesT1 = valuesT1;
    }

    public void setValuesT2(List<JoinedItems> valuesT2) {
        this.valuesT2 = valuesT2;
    }

    public List<JoinedItems> getValuesT1() {
        return valuesT1;
    }

    public List<JoinedItems> getValuesT2() {
        return valuesT2;
    }
}
