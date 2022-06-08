package uni.freiburg.sparqljoin.model.db;

import java.util.HashMap;

/**
 * Dictionary object used to map string terms to unique integers to compress operations
 */
public class Dictionary {

    private final HashMap<Long, String> values;

    private final HashMap<String, Long> invertedValues;

    private long index;

    public Dictionary() {
        this.values = new HashMap<>();
        this.invertedValues = new HashMap<>();
        this.index = 1;
    }

    /**
     * Put value into the dictionary and get unique integer instead
     * @param value to save
     * @return unique integer that represent putted value
     */
    public long put(String value) {
        // save value into the dictionary
        if (!this.invertedValues.containsKey(value)) {
            this.values.put(this.index, value);
            this.invertedValues.put(value, this.index);
            return this.index++;
        }
        // return existing index for the given value
        return this.invertedValues.get(value);
    }

    /**
     * Get string value from the dictionary by its index
     * @param key index representation of the value
     * @return value from the dictionary
     */
    public String get(long key) {
        return this.values.get(key);
    }

    public boolean containsKey(long key) {
        return this.values.containsKey(key);
    }

    public HashMap<Long, String> getValues() {
        return values;
    }
}
