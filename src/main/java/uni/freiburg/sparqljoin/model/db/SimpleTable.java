package uni.freiburg.sparqljoin.model.db;

import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Table that holds single property with simple Integer subject-object property values
 */
public class SimpleTable{

    private final String property;

    private final PropertyValues<Item<Integer>> items;

    private final Dictionary dictionary;

    public SimpleTable(String property) {
        this.property = property;
        this.items = new PropertyValues<>();
        this.dictionary = new Dictionary();
    }

    public SimpleTable(String property, Dictionary dictionary) {
        this.property = property;
        this.dictionary = dictionary;
        this.items = new PropertyValues<>();
    }

    public SimpleTable(String property, Dictionary dictionary, PropertyValues<Item<Integer>> items) {
        this.property = property;
        this.dictionary = dictionary;
        this.items = items;
    }

    public String getProperty() {
        return property;
    }

    /**
     * Save value
     * @param item to save
     */
    public void insert(Item<Integer> item) {
        this.items.put(item);
    }

    /**
     * Get all values in the table
     * @return List of values
     */
    public List<Item<Integer>> list() {
        return this.items.getValues();
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    /**
     * Transform simple table to complex one
     * @return ComplexTable
     */
    public ComplexTable toComplex() {
        List<JoinedItems> values = list().stream().map((item) ->  {
            HashMap<String, Item<Integer>> itemMap = new HashMap<>();
            itemMap.put(getProperty(), item);
            return new JoinedItems(item.subject(), itemMap);
        }).collect(Collectors.toList());
        return new ComplexTable(new LinkedHashSet<>(List.of(this.getProperty())), getDictionary(), new PropertyValues<>(values));
    }
}
