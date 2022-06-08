package uni.freiburg.sparqljoin.model.join;

import uni.freiburg.sparqljoin.model.db.Item;

import java.util.HashMap;

public record JoinedItems(long subject, HashMap<String, Item<Integer>> values) {

}
