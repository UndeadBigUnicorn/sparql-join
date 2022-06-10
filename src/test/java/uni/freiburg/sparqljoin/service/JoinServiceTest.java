package uni.freiburg.sparqljoin.service;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class JoinServiceTest {

    @InjectMocks
    JoinService joinService;

    @Test
    public void testJoin() {
        Database database = new Database(initTables());

        // join userId on givenName

        Dictionary expectedJoinedUserIdGivenNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameDict.put("HAN");
        expectedJoinedUserIdGivenNameDict.put("LEA");
        ComplexTable expectedJoinedUserIdGivenNameTable = new ComplexTable(new ArrayList<>(List.of("wsdbm:userId", "foaf:givenName")), expectedJoinedUserIdGivenNameDict);

        HashMap<String, Item<Integer>> joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        expectedJoinedUserIdGivenNameTable.insert(new JoinedItems(0, joinedValue1));
        HashMap<String, Item<Integer>> joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue2.put("foaf:givenName", new Item<>(2, 2));
        expectedJoinedUserIdGivenNameTable.insert(new JoinedItems(2, joinedValue2));
        HashMap<String, Item<Integer>> joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(24, 15125125));
        joinedValue3.put("foaf:givenName", new Item<>(24, 3));
        expectedJoinedUserIdGivenNameTable.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.join(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName"),
                "wsdbm:userId",
                "subject",
                "subject");

        compareTables(expectedJoinedUserIdGivenNameTable, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        Dictionary expectedJoinedUserIdGivenNameFamilyNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("HAN");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LEA");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SKYWALKER");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SOLO");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("ORGANA");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameTable = new ComplexTable(new ArrayList<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName")),
                expectedJoinedUserIdGivenNameFamilyNameDict);

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        expectedJoinedUserIdGivenNameFamilyNameTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue2.put("foaf:givenName", new Item<>(2, 2));
        joinedValue2.put("foaf:familyName", new Item<>(2, 5));
        expectedJoinedUserIdGivenNameFamilyNameTable.insert(new JoinedItems(2, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(24, 15125125));
        joinedValue3.put("foaf:givenName", new Item<>(24, 3));
        joinedValue3.put("foaf:familyName", new Item<>(24, 6));
        expectedJoinedUserIdGivenNameFamilyNameTable.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.join(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName"),
                "wsdbm:userId",
                "subject",
                "subject");

        compareTables(expectedJoinedUserIdGivenNameFamilyNameTable, actualJoinedUserIdGivenNameFamilyNameTable);
    }

    private void compareTables(ComplexTable expected, ComplexTable actual) {
        // check sizes
        Assert.assertEquals(String.format("Joined Table size should be %d, got %d", expected.list().size(), actual.list().size()),
                expected.list().size(), actual.list().size());
        // check properties
        Assert.assertEquals(String.format("Joined Table should have properties '%s', got '%s'", expected.getProperties(), actual.getProperties()),
                expected.getProperties(), actual.getProperties());
        // check dictionaries
        var expectedDictValues = expected.getDictionary().getValues();
        var actualDictValues = actual.getDictionary().getValues();
        Assert.assertEquals("Joined Table should have correct ordered dictionary with correct values",
                expectedDictValues, actualDictValues);
        // check values
        Assert.assertEquals("Joined Table should have correctly joined values with correct structure",
                expected.list(), actual.list());
    }

    private HashMap<String, SimpleTable> initTables() {
        HashMap<String, SimpleTable> tables = new HashMap<>();

        Dictionary givenNameDict = new Dictionary();
        givenNameDict.put("LUKE");
        givenNameDict.put("HAN");
        givenNameDict.put("LEA");
        SimpleTable givenNameTable = new SimpleTable("foaf:givenName", givenNameDict);
        givenNameTable.insert(new Item<>(0, 1));
        givenNameTable.insert(new Item<>(2, 2));
        givenNameTable.insert(new Item<>(24, 3));
        tables.put("foaf:givenName", givenNameTable);

        Dictionary familyNameDict = new Dictionary();
        familyNameDict.put("SKYWALKER");
        familyNameDict.put("SOLO");
        familyNameDict.put("ORGANA");
        SimpleTable familyNameTable = new SimpleTable("foaf:familyName", familyNameDict);
        familyNameTable.insert(new Item<>(0, 1));
        familyNameTable.insert(new Item<>(2, 2));
        familyNameTable.insert(new Item<>(24, 3));
        tables.put("foaf:familyName", familyNameTable);

        SimpleTable userIdTable = new SimpleTable("wsdbm:userId");
        userIdTable.insert(new Item<>(0, 1806723));
        userIdTable.insert(new Item<>(2, 1936247));
        userIdTable.insert(new Item<>(24, 15125125));
        tables.put("wsdbm:userId", userIdTable);

        SimpleTable followsTable = new SimpleTable("wsdbm:follows");
        followsTable.insert(new Item<>(0, 24));
        followsTable.insert(new Item<>(0, 27));
        followsTable.insert(new Item<>(2, 24));
        tables.put("wsdbm:follows", followsTable);

        SimpleTable likesTable = new SimpleTable("wsdbm:likes");
        likesTable.insert(new Item<>(24, 25));
        tables.put("wsdbm:likes", likesTable);

        return tables;
    }
}