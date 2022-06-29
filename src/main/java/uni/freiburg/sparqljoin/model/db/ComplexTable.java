package uni.freiburg.sparqljoin.model.db;

import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.List;
import java.util.stream.Stream;

/**
 * Table that holds multiple properties with complex subject-object property values
 * See SimpleTable.toComplex() for how this works.
 */
public class ComplexTable {

    private final List<String> properties;

    private final PropertyValues<JoinedItems> items;

    private final Dictionary dictionary;

    public ComplexTable(List<String> properties) {
        this.properties = properties;
        this.items = new PropertyValues<>();
        this.dictionary = new Dictionary();
    }

    public ComplexTable(List<String> properties, Dictionary dictionary) {
        this.properties = properties;
        this.dictionary = dictionary;
        this.items = new PropertyValues<>();
    }

    public ComplexTable(List<String> properties, Dictionary dictionary, PropertyValues<JoinedItems> items) {
        this.properties = properties;
        this.items = items;
        this.dictionary = dictionary;
    }

    public List<String> getProperties() {
        return properties;
    }

    /**
     * Save value
     * @param item to save
     */
    public void insert(JoinedItems item) {
        this.items.put(item);
    }

    /**
     * Get all values in the table
     * @return List of values
     */
    public List<JoinedItems> getValues() {
        return this.items.getValues();
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    /**
     * Join 2 tables
     * @param another table to join
     * @param newDict joined dictionary
     * @param values  new property values
     * @return        joined ComplexTable
     */
    public ComplexTable join(ComplexTable another, Dictionary newDict, PropertyValues<JoinedItems> values) {
        // concat properties of 2 tables
        List<String> properties = Stream.concat(this.getProperties().stream(), another.getProperties().stream()).toList();
        return new ComplexTable(properties, newDict, values);
    }

}
