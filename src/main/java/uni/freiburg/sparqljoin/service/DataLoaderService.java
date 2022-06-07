package uni.freiburg.sparqljoin.service;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uni.freiburg.sparqljoin.model.db.Database;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.db.Item;
import uni.freiburg.sparqljoin.model.db.Table;
import uni.freiburg.sparqljoin.model.parser.Triplet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

@Service
public class DataLoaderService {

    private static final Logger LOG = LoggerFactory.getLogger(DataLoaderService.class);

    private HashMap<String, Table<Integer, Integer>> tables;

    /**
     * Parse the dataset and load data into database structure
     * @param path  to the dataset to read
     */
    public Database<Integer, Integer> load(String path) {
        LOG.debug("Loading dataset...");
        tables = new HashMap<>();
        try (Stream<String> lines = Files.lines(Path.of(path))) {
            for (String line : (Iterable<String>) lines::iterator) {
                Triplet triplet = parseTriplet(line);
                processTriplet(triplet);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Database<>(tables);
    }

    /**
     * Parse line into triplet
     * @param line  to parse
     */
    private Triplet parseTriplet(final String line) {
        List<String> tokens = List.of(line.split("\t"));
        // object   property    subject .
        assert tokens.size() == 3;
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
            tables.put(triplet.property(), new Table<>(triplet.property()));
        }
        Table<Integer, Integer> table = tables.get(triplet.property());
        Dictionary dict = table.getDictionary();

        int subjectKey = extractKey(triplet.subject(), dict);
        int objectKey = extractKey(triplet.object(), dict);

        table.insert(new Item<>(subjectKey, objectKey));

    }

    /**
     * Extract integer representation from string value
     * @param value to extract key from
     * @return integer representation
     */
    private int extractKey(String value, Dictionary dict) {
        return switch (typeOf(value)) {
            case "string" ->
                    dict.put(value);
            case "int" -> Integer.parseInt(value);
            case "object" -> Integer.parseInt(value.replaceAll("\\D", ""));
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
