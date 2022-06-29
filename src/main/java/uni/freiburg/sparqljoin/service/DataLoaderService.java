package uni.freiburg.sparqljoin.service;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.parser.Triplet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

@Service
public class DataLoaderService {

    private static final Logger LOG = LoggerFactory.getLogger(DataLoaderService.class);

    private HashMap<String, SimpleTable> tables;

    /**
     * Parse the dataset and load data into database structure
     * @param path  to the dataset to read
     */
    public Database load(String path) {
        LOG.info("Loading dataset...");
        tables = new HashMap<>();
        try (Stream<String> lines = Files.lines(Path.of(path))) {
            for (String line : (Iterable<String>) lines::iterator) {
                Triplet triplet = parseTriplet(line);
                processTriplet(triplet);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Database(tables);
    }

    /**
     * Parse line into triplet
     * @param line  to parse
     */
    private Triplet parseTriplet(final String line) {
        List<String> tokens = new ArrayList<>(List.of(line.split("\t")));
        // object   property    subject .
        assert tokens.size() == 3;
        // 10M dataset looks different. It contains links instead of domain:element mapping
        if (tokens.get(0).startsWith("<http")) {
            // transform each token from http...domain/element format to domain:element
            for (int i = 0; i < tokens.size(); i++) {
                String token = tokens.get(i);
                List<String> domains = List.of(token.split("/"));
                String domain = domains.get(domains.size()-1).replaceAll(">", "");
                // needed value could be in last domain with #begin
                if (domain.contains("#")) {
                    domains = List.of(domain.split("#"));
                    domain = domains.get(domains.size()-1);
                }
                // small hash to indicate that it is an object
                if (token.contains("wsdbm")) {
                    domain = "wsdbm:" + domain;
                }
                if (token.contains("foaf")) {
                    domain = "foaf:" + domain;
                }
                if (token.contains("rev")) {
                    domain = "rev:" + domain;
                }
                tokens.set(i, domain);
            }

        }
        return Triplet.builder()
                .subject(tokens.get(0))
                .property(tokens.get(1))
                .object(tokens.get(2).split(" ")[0].replaceAll("\"", ""))
                .build();
    }

    /**
     * Put triplet into the property table,
     * save string value to dictionary if needed
     * @param triplet  to put
     */
    private void processTriplet(final Triplet triplet) {
        // create new table
        if (!tables.containsKey(triplet.property())) {
            tables.put(triplet.property(), new SimpleTable(triplet.property()));
        }
        SimpleTable table = tables.get(triplet.property());
        Dictionary dict = table.getDictionary();

        long subjectKey = extractKey(triplet.subject(), dict);
        int objectKey = (int) extractKey(triplet.object(), dict);

        table.insert(new Item<>(subjectKey, objectKey));

    }

    /**
     * Extracts the integer representation from a value.
     * Because integer comparisons are faster than string comparisons, we will assign a unique integer to each unique
     * string value and store it in dict.
     * If value is an integer wrapped inside a string, convert it to integer.
     * @param value Value to extract the key from
     * @param dict The dictionary where the mapping between string and int will be stored
     * @return integer representation
     */
    private long extractKey(String value, Dictionary dict) {
        return switch (typeOf(value)) {
            case "string" ->
                    dict.put(value);
            case "int" -> Integer.parseInt(value);
            case "object" -> Integer.parseInt(value.replaceAll("\\D", "")); // TODO may this cause an integer value collision? If not, add a comment why
            default -> throw new IllegalStateException("Unexpected value: " + typeOf(value));
        };
    }


    /**
     * Get real type of the String value
     * @param value String value to check
     * @return String representation of type
     */
    private static String typeOf(String value) {
        if (value.contains("wsdbm:")) {
            return "object";
        } else if (StringUtils.isNumeric(value)) {
            return "int";
        }
        return "string";
    }

}
