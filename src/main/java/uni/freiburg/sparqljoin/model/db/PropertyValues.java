package uni.freiburg.sparqljoin.model.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of property table values
 * @param <V> Object type
 */
public class PropertyValues<V> {

    private final List<V> values;

    public PropertyValues() {
        this.values = new ArrayList<>();
    }

    public PropertyValues(List<V> values) {
        this.values = values;
    }

    public void put(V item) {
        values.add(item);
    }

    public List<V> getValues() {
        return values;
    }
}
