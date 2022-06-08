package uni.freiburg.sparqljoin.model.db;

import lombok.Builder;
import lombok.Setter;

/**
 * Tuple representation of the PropertyValue
 * @param subject key
 * @param object  value
 */
@Builder
public record Item<V>(long subject, V object) {
}
