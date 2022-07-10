package uni.freiburg.sparqljoin.model.db;

import java.util.HashMap;

/**
 * Dictionary object used to map string terms to unique integers to compress operations
 */
public class Dictionary {

    private final HashMap<Integer, String> values;

    private final HashMap<String, Integer> invertedValues;

    private int index;

    public Dictionary() {
        this.values = new HashMap<>();
        this.invertedValues = new HashMap<>();
        this.index = 1;
    }

    public Dictionary(HashMap<Integer, String> values, HashMap<String, Integer> invertedValues, int index) {
        this.values = values;
        this.invertedValues = invertedValues;
        this.index = index;
    }

    /**
     * Put value into the dictionary (if not exists) and return unique integer representation
     *
     * @param value to save
     * @return unique integer that represent putted value
     */
    public int put(String value) {
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
    public String get(int key) {
        return this.values.get(key);
    }

    public boolean containsKey(int key) {
        return this.values.containsKey(key);
    }

    public HashMap<Integer, String> getValues() {
        return values;
    }

    public HashMap<String, Integer> getInvertedValues() {
        return invertedValues;
    }

    public void putAll(Dictionary otherDictionary) {
        otherDictionary.getValues().forEach((otherDictionaryIndex, value) -> this.put(value));
    }

    @Override
    public String toString() {
        return this.getValues().toString();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Dictionary clone() {
        //noinspection unchecked
        return new Dictionary(
                (HashMap<Integer, String>) this.values.clone(),
                (HashMap<String, Integer>) this.invertedValues.clone(),
                this.index);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != Dictionary.class) return false;

        Dictionary other = (Dictionary) obj;
        return this.values.equals(other.values) &&
                this.invertedValues.equals(other.invertedValues) &&
                this.index == other.index;
    }
}
