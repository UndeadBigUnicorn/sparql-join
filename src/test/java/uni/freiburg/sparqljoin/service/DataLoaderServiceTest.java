package uni.freiburg.sparqljoin.service;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import uni.freiburg.sparqljoin.model.db.*;

import java.util.HashMap;

@RunWith(MockitoJUnitRunner.class)
public class DataLoaderServiceTest {

    private static final String DATASET_PATH = "dataset/test.txt";

    @InjectMocks
    DataLoaderService dataLoaderService;

    @Test
    public void testLoadDataset() {
        Database expected = new Database(initTables());
        Database actual = dataLoaderService.load(DATASET_PATH);

        Assert.assertEquals("Some table is missing", expected.tables().size(), actual.tables().size());

        actual.tables().forEach((key, actualTable) -> Assert.assertEquals(String.format("For key '%s' tables are not equal", key),
                expected.tables().get(key).list(), actualTable.list()));
    }

    private HashMap<String, Dictionary> initDictionaries() {
        HashMap<String, Dictionary> dictionaries = new HashMap<>();
        Dictionary emailDict = new Dictionary();
        emailDict.put("example@gmail.com");
        dictionaries.put("sorg:email", emailDict);

        Dictionary givenNameDict = new Dictionary();
        givenNameDict.put("BERTA");
        dictionaries.put("foaf:givenName", givenNameDict);

        Dictionary familyNameDict = new Dictionary();
        familyNameDict.put("LEAH");
        dictionaries.put("foaf:familyName", familyNameDict);
        return dictionaries;
    }

    private HashMap<String, SimpleTable> initTables() {
        HashMap<String, Dictionary> dictionaries = initDictionaries();
        HashMap<String, SimpleTable> tables = new HashMap<>();

        SimpleTable parentCountryTable = new SimpleTable("gn:parentCountry");
        parentCountryTable.insert(new Item(0, 20, DataType.OBJECT));
        parentCountryTable.insert(new Item(1, 0, DataType.OBJECT));
        tables.put("gn:parentCountry", parentCountryTable);

        SimpleTable followsTable = new SimpleTable("wsdbm:follows");
        followsTable.insert(new Item(0, 24, DataType.OBJECT));
        followsTable.insert(new Item(0, 27, DataType.OBJECT));
        tables.put("wsdbm:follows", followsTable);

        SimpleTable userIdTable = new SimpleTable("wsdbm:userId");
        userIdTable.insert(new Item(0, 1806723, DataType.INTEGER));
        userIdTable.insert(new Item(2, 1936247, DataType.INTEGER));
        tables.put("wsdbm:userId", userIdTable);

        SimpleTable emailTable = new SimpleTable("sorg:email", dictionaries.get("sorg:email"));
        emailTable.insert(new Item(0, 1, DataType.STRING));
        tables.put("sorg:email", emailTable);

        SimpleTable givenNameTable =
                new SimpleTable("foaf:givenName", dictionaries.get("foaf:givenName"));
        givenNameTable.insert(new Item(0, 1, DataType.STRING));
        givenNameTable.insert(new Item(2, 1, DataType.STRING));
        tables.put("foaf:givenName", givenNameTable);

        SimpleTable familyNameTable =
                new SimpleTable("foaf:familyName", dictionaries.get("foaf:familyName"));
        familyNameTable.insert(new Item(2, 1, DataType.STRING));
        tables.put("foaf:familyName", familyNameTable);
        return tables;
    }

}