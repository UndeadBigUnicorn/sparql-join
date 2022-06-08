package uni.freiburg.sparqljoin.model.db;

import java.util.List;

/**
 * Table object representation. This class is used to hold property values
 * @param <V> Type of values hold by table
 */
public abstract class Table<V> {

    private PropertyValues<V> items;

    private Dictionary dictionary;



    /**
     * Save value
     * @param item to save
     */
    public void insert(V item) {
        this.items.put(item);
    }

    /**
     * Get all values in the table
     * @return List of values
     */
    public List<V> list() {
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
     * @param <L>     Type of values hold by another table
     * @param <T>     Type of values that would have new joined table
     */
//    abstract <L, T> Table<T> join(Table<L> another, Dictionary newDict, PropertyValues<T> values);

}
