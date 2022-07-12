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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database expectedDatabase = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        Database actualDatabase = dataLoaderService.load(DATASET_PATH);

        Assert.assertEquals("Some table is missing", expectedDatabase.tables().size(), actualDatabase.tables().size());

        actualDatabase.tables().forEach((key, actualTable) -> Assert.assertEquals(String.format("For key '%s' tables are not equal", key),
                expectedDatabase.tables().get(key).list(), actualTable.list()));
    }

    private Dictionary initExpectedObjectDictionary() {
        Dictionary dictionary = new Dictionary();
        dictionary.put("example@gmail.com");
        dictionary.put("BERTA");
        dictionary.put("LEAH");
        return dictionary;
    }

    private Dictionary initExpectedPropertyDictionary() {
        Dictionary propertyDictionary = new Dictionary();
        propertyDictionary.put("gn:parentCountry");
        propertyDictionary.put("wsdbm:follows");
        propertyDictionary.put("wsdbm:userId");
        propertyDictionary.put("sorg:email");
        propertyDictionary.put("foaf:givenName");
        propertyDictionary.put("foaf:familyName");
        return propertyDictionary;
    }

    private PreparedDataset initExpectedDataset() {
        Dictionary propertyDictionary = initExpectedPropertyDictionary();
        Dictionary objectDictionary = initExpectedObjectDictionary();
        HashMap<String, SimpleTable> tables = new HashMap<>();

        SimpleTable parentCountryTable = new SimpleTable("gn:parentCountry", propertyDictionary, objectDictionary);
        parentCountryTable.insert(new Item(0, 20, DataType.OBJECT));
        parentCountryTable.insert(new Item(1, 0, DataType.OBJECT));
        tables.put("gn:parentCountry", parentCountryTable);

        SimpleTable followsTable = new SimpleTable("wsdbm:follows", propertyDictionary, objectDictionary);
        followsTable.insert(new Item(10, 24, DataType.OBJECT));
        followsTable.insert(new Item(10, 27, DataType.OBJECT));
        tables.put("wsdbm:follows", followsTable);

        SimpleTable userIdTable = new SimpleTable("wsdbm:userId", propertyDictionary, objectDictionary);
        userIdTable.insert(new Item(10, 1806723, DataType.INTEGER));
        userIdTable.insert(new Item(12, 1936247, DataType.INTEGER));
        tables.put("wsdbm:userId", userIdTable);

        SimpleTable emailTable = new SimpleTable("sorg:email", propertyDictionary, objectDictionary);
        emailTable.insert(new Item(0, 1, DataType.STRING));
        tables.put("sorg:email", emailTable);

        SimpleTable givenNameTable = new SimpleTable("foaf:givenName", propertyDictionary, objectDictionary);
        givenNameTable.insert(new Item(0, 2, DataType.STRING));
        givenNameTable.insert(new Item(2, 2, DataType.STRING));
        tables.put("foaf:givenName", givenNameTable);

        SimpleTable familyNameTable = new SimpleTable("foaf:familyName", propertyDictionary, objectDictionary);
        familyNameTable.insert(new Item(12, 3, DataType.STRING));
        tables.put("foaf:familyName", familyNameTable);

        return new PreparedDataset(tables, propertyDictionary, objectDictionary);
    }

}