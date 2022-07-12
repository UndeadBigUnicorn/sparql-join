package uni.freiburg.sparqljoin.model.db;

import uni.freiburg.sparqljoin.model.join.JoinedItems;
import uni.freiburg.sparqljoin.model.join.JoinedItemsOptimized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Table that stores property values in columns instead of rows
 * @param dictionaries  reference to dictionaries
 * @param propertyItems references to property values that are stored as columns
 */
public record VerticallyPartitionedTable(HashMap<String, Dictionary> dictionaries,
                                         HashMap<String, PropertyValues<Item>> propertyItems) {

    /**
     * Get all properties that are presented in the table
     * @return list of properties
     */
    public List<String> getProperties() {
        return propertyItems.keySet().stream().toList();
    }

    public int size() {
        return propertyItems.values().stream().toList().get(0).getValues().size();
    }

    /**
     * Join 2 tables, merge dictionaries references, and return a new one
     * @param other         VerticallyPartitionedTable for the reference to join
     * @param propertyItems that new VerticallyPartitionedTable should hold
     * @return new VerticallyPartitionedTable
     */
    public VerticallyPartitionedTable join(VerticallyPartitionedTable other, HashMap<String, PropertyValues<Item>> propertyItems) {
        // merge hash map of dictionaries
        HashMap<String, Dictionary> newDicts = new HashMap<>();
        newDicts.putAll(this.dictionaries());
        newDicts.putAll(other.dictionaries());
        return new VerticallyPartitionedTable(newDicts, propertyItems);
    }

    /**
     * Convert values from vertically partition to horizontal one
     * @return List<JoinedItems>
     */
    public List<JoinedItemsOptimized> itemsToHorizontalPartition() {
        List<JoinedItemsOptimized> joinedItems = new ArrayList<>();

        for (int i = 0; i < this.size(); i++) {
            HashMap<String, Item> propertyItems = new HashMap<>();
            int finalI = i;
            this.propertyItems.forEach((property, propertyValues) -> {
                propertyItems.put(property, propertyValues.getValues().get(finalI));
            });
            joinedItems.add(new JoinedItemsOptimized(i, propertyItems));
        }

        return joinedItems;
    }

    /**
     * Put all values from other table
     * @param other VerticallyPartitionedTable for the reference
     */
    public void putAll(VerticallyPartitionedTable other) {
        other.propertyItems().forEach(
                (property, propertyValues) -> this.propertyItems.get(property).getValues().addAll(propertyValues.getValues()));
    }
}
