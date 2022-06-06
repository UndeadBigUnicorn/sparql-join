package uni.freiburg.sparqljoin.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import uni.freiburg.sparqljoin.model.db.Database;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.db.Item;
import uni.freiburg.sparqljoin.model.db.Table;

import java.util.HashMap;

@RunWith(MockitoJUnitRunner.class)
public class DataLoaderServiceTest {

    private static final String DATASET_PATH = "dataset/test.txt";

    @InjectMocks
    DataLoaderService dataLoaderService;

    @Before
    public void setup() {
        initMocks();
    }

    @Test
    public void testLoadDataset() {
        Database expected = new Database(initTables(), initDictionaries());
        Database actual = dataLoaderService.load(DATASET_PATH);

        Assert.assertEquals("Some table is missing", expected.tables().size(), actual.tables().size());

        actual.tables().forEach((key, actualTable) -> {
            Assert.assertEquals(String.format("For key '%s' tables are not equal", key),
                    expected.tables().get(key).list(), actualTable.list());
        });
    }

    private void initMocks() {
        Whitebox.setInternalState(dataLoaderService, "dictionaries", initDictionaries());
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

    private HashMap<String, Table<Integer, Integer>> initTables() {
        HashMap<String, Table<Integer, Integer>> tables = new HashMap<>();

        Table<Integer, Integer> parentCountryTable = new Table<>("gn:parentCountry");
        parentCountryTable.insert(new Item<>(0, 20));
        parentCountryTable.insert(new Item<>(1, 0));
        tables.put("gn:parentCountry", parentCountryTable);

        Table<Integer, Integer> followsTable = new Table<>("wsdbm:follows");
        followsTable.insert(new Item<>(0, 24));
        followsTable.insert(new Item<>(0, 27));
        tables.put("wsdbm:follows", followsTable);

        Table<Integer, Integer> userIdTable = new Table<>("wsdbm:userId");
        userIdTable.insert(new Item<>(0, 1806723));
        userIdTable.insert(new Item<>(2, 1936247));
        tables.put("wsdbm:userId", userIdTable);

        Table<Integer, Integer> emailTable = new Table<>("sorg:email");
        emailTable.insert(new Item<>(0, 1));
        tables.put("sorg:email", emailTable);

        Table<Integer, Integer> givenNameTable = new Table<>("foaf:givenName");
        givenNameTable.insert(new Item<>(0, 1));
        givenNameTable.insert(new Item<>(2, 1));
        tables.put("foaf:givenName", givenNameTable);

        Table<Integer, Integer> familyNameTable = new Table<>("foaf:familyName");
        familyNameTable.insert(new Item<>(2, 1));
        tables.put("foaf:familyName", familyNameTable);
        return tables;
    }




}