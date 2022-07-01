package uni.freiburg.sparqljoin.model.db;

import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Table that holds multiple properties with complex subject-object property values
 * See SimpleTable.toComplex() for how this works.
 */
public class ComplexTable {

    private final Set<String> properties;

    private final PropertyValues<JoinedItems> items;

    private final Dictionary dictionary;

    public ComplexTable(Set<String> properties) {
        this.properties = properties;
        this.items = new PropertyValues<>();
        this.dictionary = new Dictionary();
    }

    public ComplexTable(Set<String> properties, Dictionary dictionary) {
        this.properties = properties;
        this.dictionary = dictionary;
        this.items = new PropertyValues<>();
    }

    public ComplexTable(Set<String> properties, Dictionary dictionary, PropertyValues<JoinedItems> items) {
        this.properties = properties;
        this.items = items;
        this.dictionary = dictionary;
    }

    public Set<String> getProperties() {
        return properties;
    }

    /**
     * Insert items, dictionary values must already exist
     *
     * @param items to save
     */
    public void insert(JoinedItems items) {
        this.items.put(items);

        // Add properties if not exists
        this.properties.addAll(items.values().keySet());
    }

    /**
     * Insert items, adding missing dictionary entries
     *
     * @param items to save
     */
    public void insert(JoinedItems items, Dictionary dictionary) {
        insert(items);

        // Add dictionary entries if not exists
        this.dictionary.insertValues(dictionary);
    }

    /**
     * Adds the data of another ComplexTable into this one
     *
     * @param otherTable The other table
     */
    public void insertComplexTable(ComplexTable otherTable) {
        for (JoinedItems item : otherTable.getValues()) {
            this.items.put(item);

            // Add properties if not exists
            this.properties.addAll(item.values().keySet());
        }

        // Add missing dictionary entries
        this.dictionary.insertValues(otherTable.getDictionary());
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
        Set<String> properties = Stream.concat(this.getProperties().stream(), another.getProperties().stream()).collect(Collectors.toSet());
        return new ComplexTable(properties, newDict, values);
    }

}
