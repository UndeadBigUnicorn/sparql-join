package uni.freiburg.sparqljoin.model.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of property table values
 * @param <K> Subject
 * @param <V> Object
 */
public class PropertyValues<K, V> {

    private List<Item<K, V>> values;

    public PropertyValues() {
        this.values = new ArrayList<>();
    }

    public void put(Item<K, V> item) {
        values.add(item);
    }

    public List<Item<K, V>> getValues() {
        return values;
    }
}
