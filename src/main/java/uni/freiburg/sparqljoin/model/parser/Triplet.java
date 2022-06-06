package uni.freiburg.sparqljoin.model.parser;

import lombok.Builder;

@Builder
public record Triplet(String property, String subject, String object) {

}
