package uni.freiburg.sparqljoin.model.db;

import java.util.HashMap;

public record Database (HashMap<String, Table<Integer, Integer>> tables, HashMap<String, Dictionary> dictionaries) {
}
