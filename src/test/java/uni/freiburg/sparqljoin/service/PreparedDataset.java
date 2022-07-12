package uni.freiburg.sparqljoin.service;

import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.db.SimpleTable;

import java.util.HashMap;

public record PreparedDataset(HashMap<String, SimpleTable> simpleTables, Dictionary propertyDictionary,
                              Dictionary objectDictionary) {
}
