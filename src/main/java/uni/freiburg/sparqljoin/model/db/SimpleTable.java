package uni.freiburg.sparqljoin.model.db;

import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Table that holds single property with simple Integer subject-object property values
 */
public class SimpleTable {

    private final String property;

    private final PropertyValues<Item> items;

    private final Dictionary objectDictionary;

    public SimpleTable(String property) {
        this.property = property;
        this.items = new PropertyValues<>();
        this.objectDictionary = new Dictionary();
    }

    public SimpleTable(String property, Dictionary objectDictionary) {
        this.property = property;
        this.objectDictionary = objectDictionary;
        this.items = new PropertyValues<>();
    }

    public SimpleTable(String property, Dictionary objectDictionary, PropertyValues<Item> items) {
        this.property = property;
        this.objectDictionary = objectDictionary;
        this.items = items;
    }

    public String getProperty() {
        return property;
    }

    /**
     * Save value
     *
     * @param item to save
     */
    public void insert(Item item) {
        this.items.put(item);
    }

    /**
     * Get all values in the table
     *
     * @return List of values
     */
    public List<Item> list() {
        return this.items.getValues();
    }

    public Dictionary getObjectDictionary() {
        return objectDictionary;
    }

    /**
     * Transform simple table to complex one
     *
     * @return ComplexTable
     */
    public ComplexTable toComplex() {
        Dictionary propertyDictionary = new Dictionary();
        Integer propertyInteger = propertyDictionary.put(getProperty());
        List<JoinedItems> values = list().stream().map((item) -> {
            HashMap<Integer, Item> itemMap = new HashMap<>();
            itemMap.put(propertyInteger, item);
            return new JoinedItems(item.subject(), itemMap);
        }).collect(Collectors.toList());
        return new ComplexTable(propertyDictionary, getObjectDictionary(), new PropertyValues<>(values));
    }
}
