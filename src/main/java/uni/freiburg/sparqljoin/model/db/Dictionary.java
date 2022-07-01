package uni.freiburg.sparqljoin.model.db;

import org.apache.commons.lang3.NotImplementedException;

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
     * Put value into the dictionary (if not exists) and return unique integer representation
     *
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
     *
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

    public void insertValues(Dictionary otherDictionary) {
        otherDictionary.getValues().forEach((key, value) -> {
            String dictionaryValue = this.values.get(key);

            if (dictionaryValue == null) {
                // Key-value pair does not exist yet
                this.values.put(key, value);

                this.invertedValues.put(value, key); // Assume values and invertedValues are always kept up to date, therefore no need to check existence in invertedValues
            } else {
                // Check if value matches
                if(!dictionaryValue.equals(value)) {
                    // TODO need to change keys in data structure
                    throw new NotImplementedException();
                }
            }
        });
    }
}
