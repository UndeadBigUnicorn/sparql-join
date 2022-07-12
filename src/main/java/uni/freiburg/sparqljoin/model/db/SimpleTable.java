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

    private final Dictionary propertyDictionary;

    private final Dictionary objectDictionary;

    public SimpleTable(String property, Dictionary propertyDictionary, Dictionary objectDictionary) {
        this.property = property;
        this.propertyDictionary = propertyDictionary;
        this.objectDictionary = objectDictionary;
        this.items = new PropertyValues<>();
    }

    public SimpleTable(String property, Dictionary propertyDictionary, Dictionary objectDictionary, PropertyValues<Item> items) {
        this.property = property;
        this.propertyDictionary = propertyDictionary;
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

    public Dictionary getPropertyDictionary() {
        return propertyDictionary;
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
        Integer propertyInteger = propertyDictionary.getInvertedValues().get(getProperty());

        List<JoinedItems> values = list().stream().map((item) -> {
            HashMap<Integer, Item> itemMap = new HashMap<>();
            itemMap.put(propertyInteger, item);
            return new JoinedItems(item.subject(), itemMap);
        }).collect(Collectors.toList());

        return new ComplexTable(getPropertyDictionary(), getObjectDictionary(), new PropertyValues<>(values));
    }
}
