package uni.freiburg.sparqljoin.model.db;

import lombok.Builder;

/**
 * Tuple representation of the PropertyValue
 *
 * @param subject key
 * @param object  value
 * @param type    type of the data that object represents
 */
@Builder
public record Item<V>(long subject, V object, DataType type) {
}
