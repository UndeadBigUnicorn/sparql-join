package uni.freiburg.sparqljoin.model.db;

import java.util.List;

public class Table <K, V> {

    private final String property;

    private final PropertyValues<K, V> items;

    public Table(String property) {
        this.property = property;
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
}
