package uni.freiburg.sparqljoin.model.db;

import lombok.Builder;

/**
 * Tuple representation of the PropertyValue
 * @param subject key
 * @param object  value
 */
@Builder
public record Item <K, V>(K subject, V object) {
}
