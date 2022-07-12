package uni.freiburg.sparqljoin.model.db;

import java.util.HashMap;

/**
 * This class is used to hold collection of tables and provide operations on them
 * @param tables       collection of tables
 */
public record Database (HashMap<String, SimpleTable> tables, Dictionary propertyDictionary, Dictionary objectDictionary) {
}
