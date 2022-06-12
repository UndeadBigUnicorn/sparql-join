package uni.freiburg.sparqljoin.model.join;

import uni.freiburg.sparqljoin.model.db.Item;

import java.util.HashMap;

public record JoinedItems(long subject, HashMap<String, Item<Integer>> values){
    @Override
    public boolean equals(Object obj) {
        assert obj instanceof JoinedItems;
        JoinedItems o = (JoinedItems) obj;
        for (String property : values().keySet()) {
            if (!o.values().containsKey(property)) {
                return false;
            }
            Item<Integer> thisItem = values().get(property);
            Item<Integer> otherItem = o.values().get(property);
            if (!thisItem.equals(otherItem)) {
                return false;
            }
        }
        return true;
    }

}
