package uni.freiburg.sparqljoin.model.db;

import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.List;

/**
 * Table that holds multiple properties with complex subject-object property values
 * See SimpleTable.toComplex() for how this works.
 */
public class ComplexTable {

    private final Dictionary propertyDictionary;

    private final PropertyValues<JoinedItems> items;

    private final Dictionary objectDictionary;

    public ComplexTable(Dictionary propertyDictionary) {
        this.propertyDictionary = propertyDictionary;
        this.items = new PropertyValues<>();
        this.objectDictionary = new Dictionary();
    }

    public ComplexTable(Dictionary propertyDictionary, Dictionary objectDictionary) {
        this.propertyDictionary = propertyDictionary;
        this.objectDictionary = objectDictionary;
        this.items = new PropertyValues<>();
    }

    public ComplexTable(Dictionary propertyDictionary, Dictionary objectDictionary, PropertyValues<JoinedItems> items) {
        this.propertyDictionary = propertyDictionary;
        this.items = items;
        this.objectDictionary = objectDictionary;
    }

    public Dictionary getPropertyDictionary() {
        return propertyDictionary;
    }

    /**
     * Insert items, adding missing object dictionary entries
     *
     * @param items to save
     */
    public void insert(JoinedItems items) {
        this.items.put(items);
    }

    /**
     * Adapt object longs with values from the other dictionary and insert into this table
     *
     * @param otherTable The other table
     */
    public void insertComplexTable(ComplexTable otherTable) {
        otherTable.getValues().forEach(joinedItems -> {
            joinedItems.values().forEach((otherProperty, otherPropertyValue) -> {
                String propertyStr = otherTable.getPropertyDictionary().getValues().get(otherProperty);
                assert propertyStr != null;

                Integer propertyInteger = this.getPropertyDictionary().getInvertedValues().get(propertyStr);
                if (propertyInteger == null) {
                    propertyInteger = this.getPropertyDictionary().put(propertyStr);
                }

                int object = otherPropertyValue.object();
                if (otherPropertyValue.type() == DataType.STRING) {
                    // Object is a string
                    String itemObjectStr = otherTable.getObjectDictionary().getValues().get((long) otherPropertyValue.object());
                    Integer itemObjectKey = this.getObjectDictionary().getInvertedValues().get(itemObjectStr);
                    if (itemObjectKey == null) {
                        // Does not exist yet, generate a new value
                        itemObjectKey = this.getObjectDictionary().put(itemObjectStr);
                    }
                    object = itemObjectKey.intValue();
                }
                joinedItems.values().put(propertyInteger, new Item(otherPropertyValue.subject(), object, otherPropertyValue.type()));
            });

            this.items.put(joinedItems);
        });
    }

    /**
     * Get all values in the table
     *
     * @return List of values
     */
    public List<JoinedItems> getValues() {
        return this.items.getValues();
    }

    public Dictionary getObjectDictionary() {
        return objectDictionary;
    }
}
