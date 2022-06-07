package uni.freiburg.sparqljoin.model.db;

import java.util.List;

/**
 * Table object representation. This class is used to hold property values
 * @param <K> Type of the key for values
 * @param <V> Type of values hold by table
 */
public class Table <K, V> {

    private final String property;

    private final PropertyValues<K, V> items;

    private final Dictionary dictionary;

    public Table(String property) {
        this.property = property;
        this.items = new PropertyValues<>();
        this.dictionary = new Dictionary();
    }

    public Table(String property, Dictionary dictionary) {
        this.property = property;
        this.dictionary = dictionary;
        this.items = new PropertyValues<>();
    }

    /**
     * Save value
     * @param item to save
     */
    public void insert(Item<K, V> item) {
        this.items.put(item);
    }

    public List<Item<K, V>> list() {
        return this.items.getValues();
    }

    public Dictionary getDictionary() {
        return dictionary;
    }
}
