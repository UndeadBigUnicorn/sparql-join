package uni.freiburg.sparqljoin.service;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import uni.freiburg.sparqljoin.join.JoinOn;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.join.JoinedItems;
import uni.freiburg.sparqljoin.model.join.JoinedItemsOptimized;

import java.util.HashMap;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class JoinServiceTest {

    @InjectMocks
    JoinService joinService;

    /**
     * Test sequence hash join of 1 property tables
     */
    @Test
    public void testSimpleHashJoin() {
        Database database = new Database(initSimpleTables());

        // join userId on givenName

        Dictionary expectedJoin1PropertyDict = new Dictionary();
        expectedJoin1PropertyDict.put("wsdbm:userId");
        expectedJoin1PropertyDict.put("foaf:givenName");
        Dictionary expectedJoin1ObjectDict = new Dictionary();
        expectedJoin1ObjectDict.put("LUKE");
        expectedJoin1ObjectDict.put("HAN");
        expectedJoin1ObjectDict.put("LEA");
        ComplexTable expectedJoin1Table = new ComplexTable(expectedJoin1PropertyDict, expectedJoin1ObjectDict);

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(0, joinedValue1, true));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(2, joinedValue2, true));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(24, joinedValue3, true));

        ComplexTable actualJoin1Table = joinService.hashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoin1Table);

        // join userId, givenName on familyName

        Dictionary expectedJoin2PropertyDict = new Dictionary();
        expectedJoin2PropertyDict.put("wsdbm:userId");
        expectedJoin2PropertyDict.put("foaf:givenName");
        expectedJoin2PropertyDict.put("foaf:familyName");
        Dictionary expectedJoin2ObjectDict = new Dictionary();
        expectedJoin2ObjectDict.put("LUKE");
        expectedJoin2ObjectDict.put("HAN");
        expectedJoin2ObjectDict.put("LEA");
        expectedJoin2ObjectDict.put("SKYWALKER");
        expectedJoin2ObjectDict.put("SOLO");
        expectedJoin2ObjectDict.put("ORGANA");
        ComplexTable expectedJoin2Table = new ComplexTable(expectedJoin2PropertyDict,
                expectedJoin2ObjectDict);

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(2, joinedValue2, true));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        joinedValue3.put(3, new Item(24, 6, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(24, joinedValue3, true));

        ComplexTable actualJoin2Table = joinService.hashJoin(
                actualJoin1Table,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoin2Table);

        // join userId, givenName, familyName on follows

        Dictionary expectedJoin3PropertyDict = new Dictionary();
        expectedJoin3PropertyDict.put("wsdbm:userId");
        expectedJoin3PropertyDict.put("foaf:givenName");
        expectedJoin3PropertyDict.put("foaf:familyName");
        expectedJoin3PropertyDict.put("wsdbm:follows");
        ComplexTable expectedJoin3Table = new ComplexTable(
                expectedJoin3PropertyDict,
                expectedJoin2Table.getObjectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 4, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue2, true));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 5, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(2, joinedValue3, true));

        // subject 24 does not follow anyone, so remove givenName LEA and familyName ORGANA from the expected result dict
        expectedJoin3Table.getObjectDictionary().getValues().remove(3);
        expectedJoin3Table.getObjectDictionary().getInvertedValues().remove("LEA");
        expectedJoin3Table.getObjectDictionary().getValues().remove(6);
        expectedJoin3Table.getObjectDictionary().getInvertedValues().remove("ORGANA");

        ComplexTable actualJoin3Table = joinService.hashJoin(
                actualJoin2Table,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoin3Table, actualJoin3Table);

        // join userId, givenName, familyName, follows on likes

        Dictionary expectedJoin4PropertyDict = new Dictionary();
        expectedJoin4PropertyDict.put("wsdbm:userId");
        expectedJoin4PropertyDict.put("foaf:givenName");
        expectedJoin4PropertyDict.put("foaf:familyName");
        expectedJoin4PropertyDict.put("wsdbm:follows");
        expectedJoin4PropertyDict.put("wsdbm:likes");
        ComplexTable expectedJoin4Table = new ComplexTable(
                expectedJoin4PropertyDict,
                expectedJoin3Table.getObjectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        joinedValue1.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        joinedValue2.put(4, new Item(2, 24, DataType.OBJECT));
        joinedValue2.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(2, joinedValue2, true));

        ComplexTable actualJoin4Table = joinService.hashJoin(
                actualJoin3Table,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoin4Table, actualJoin4Table);
    }

    /**
     * Test hash join of 2 multi-properties tables
     */
    @Test
    public void testComplexHashJoin() {
        Database database = new Database(initSimpleTables());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        Dictionary expectedJoin1PropertyDict = new Dictionary();
        expectedJoin1PropertyDict.put("wsdbm:userId");
        expectedJoin1PropertyDict.put("foaf:givenName");
        expectedJoin1PropertyDict.put("foaf:familyName");
        expectedJoin1PropertyDict.put("wsdbm:follows");
        Dictionary expectedJoin1ObjectDict = new Dictionary();
        expectedJoin1ObjectDict.put("LUKE");
        expectedJoin1ObjectDict.put("HAN");
        expectedJoin1ObjectDict.put("SKYWALKER");
        expectedJoin1ObjectDict.put("SOLO");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(expectedJoin1PropertyDict,
                expectedJoin1ObjectDict);

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 3, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1, true));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 3, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2, true));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 4, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3, true));

        ComplexTable userIdGivenNameTable = joinService.hashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        ComplexTable userIdGivenNameFamilyNameTable = joinService.hashJoin(
                userIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        ComplexTable userIdGivenNameFamilyNameFollowsTable = joinService.hashJoin(
                userIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.hashJoin(
                userIdGivenNameFamilyNameTable,
                userIdGivenNameFamilyNameFollowsTable,
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:userId",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsTable, actualJoinedUserIdGivenNameFamilyNameFollowsTable);
    }

    /**
     * Test sequence parallel hash join of 1 property tables
     */
    @Test
    public void testSimpleParallelHashJoin() {
        Database database = new Database(initSimpleTables());

        // join userId on givenName

        Dictionary expectedJoin1PropertyDict = new Dictionary();
        expectedJoin1PropertyDict.put("wsdbm:userId");
        expectedJoin1PropertyDict.put("foaf:givenName");
        Dictionary expectedJoin1ObjectDict = new Dictionary();
        expectedJoin1ObjectDict.put("LUKE");
        expectedJoin1ObjectDict.put("HAN");
        expectedJoin1ObjectDict.put("LEA");
        ComplexTable expectedJoin1Table = new ComplexTable(expectedJoin1PropertyDict, expectedJoin1ObjectDict);

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(0, joinedValue1, true));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(2, joinedValue2, true));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(24, joinedValue3, true));

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        Dictionary expectedJoin2PropertyDict = new Dictionary();
        expectedJoin2PropertyDict.put("wsdbm:userId");
        expectedJoin2PropertyDict.put("foaf:givenName");
        expectedJoin2PropertyDict.put("foaf:familyName");
        Dictionary expectedJoin2ObjectDict = new Dictionary();
        expectedJoin2ObjectDict.put("LUKE");
        expectedJoin2ObjectDict.put("HAN");
        expectedJoin2ObjectDict.put("LEA");
        expectedJoin2ObjectDict.put("SKYWALKER");
        expectedJoin2ObjectDict.put("SOLO");
        expectedJoin2ObjectDict.put("ORGANA");
        ComplexTable expectedJoin2Table = new ComplexTable(expectedJoin2PropertyDict,
                expectedJoin2ObjectDict);

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(2, joinedValue2, true));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        joinedValue3.put(3, new Item(24, 6, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(24, joinedValue3, true));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        Dictionary expectedJoin3PropertyDict = new Dictionary();
        expectedJoin3PropertyDict.put("wsdbm:userId");
        expectedJoin3PropertyDict.put("foaf:givenName");
        expectedJoin3PropertyDict.put("foaf:familyName");
        expectedJoin3PropertyDict.put("wsdbm:follows");
        ComplexTable expectedJoin3Table = new ComplexTable(
                expectedJoin3PropertyDict,
                expectedJoin2Table.getObjectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 4, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue2, true));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 5, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(2, joinedValue3, true));

        // subject 24 does not follow anyone, so remove givenName LEA and familyName ORGANA from the expected result dict
        expectedJoin3Table.getObjectDictionary().getValues().remove(3);
        expectedJoin3Table.getObjectDictionary().getInvertedValues().remove("LEA");
        expectedJoin3Table.getObjectDictionary().getValues().remove(6);
        expectedJoin3Table.getObjectDictionary().getInvertedValues().remove("ORGANA");

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoin3Table, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        Dictionary expectedJoin4PropertyDict = new Dictionary();
        expectedJoin4PropertyDict.put("wsdbm:userId");
        expectedJoin4PropertyDict.put("foaf:givenName");
        expectedJoin4PropertyDict.put("foaf:familyName");
        expectedJoin4PropertyDict.put("wsdbm:follows");
        expectedJoin4PropertyDict.put("wsdbm:likes");
        ComplexTable expectedJoin4Table = new ComplexTable(
                expectedJoin4PropertyDict,
                expectedJoin3Table.getObjectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        joinedValue1.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        joinedValue2.put(4, new Item(2, 24, DataType.OBJECT));
        joinedValue2.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(2, joinedValue2, true));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameFamilyNameFollowsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoin4Table, actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable);
    }

    /**
     * Test parallel hash join of 2 multi-properties tables
     */
    @Test
    public void testComplexParallelHashJoin() {
        Database database = new Database(initSimpleTables());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        Dictionary expectedJoin1PropertyDict = new Dictionary();
        expectedJoin1PropertyDict.put("wsdbm:userId");
        expectedJoin1PropertyDict.put("foaf:givenName");
        expectedJoin1PropertyDict.put("foaf:familyName");
        expectedJoin1PropertyDict.put("wsdbm:follows");
        Dictionary expectedJoin1ObjectDict = new Dictionary();
        expectedJoin1ObjectDict.put("LUKE");
        expectedJoin1ObjectDict.put("HAN");
        expectedJoin1ObjectDict.put("SKYWALKER");
        expectedJoin1ObjectDict.put("SOLO");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(expectedJoin1PropertyDict,
                expectedJoin1ObjectDict);

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 3, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1, true));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 3, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2, true));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 4, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3, true));

        ComplexTable userIdGivenNameTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        ComplexTable userIdGivenNameFamilyNameTable = joinService.parallelHashJoin(
                userIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        ComplexTable userIdGivenNameFamilyNameFollowsTable = joinService.parallelHashJoin(
                userIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.parallelHashJoin(
                userIdGivenNameFamilyNameTable,
                userIdGivenNameFamilyNameFollowsTable,
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:userId",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsTable, actualJoinedUserIdGivenNameFamilyNameFollowsTable);
    }

    /**
     * Test sequence sort merge join of 1 property tables
     */
    @Test
    public void testSimpleSortMergeJoin() {
        Database database = new Database(initSimpleTables());

        // join userId on givenName

        Dictionary expectedJoin1PropertyDict = new Dictionary();
        expectedJoin1PropertyDict.put("wsdbm:userId");
        expectedJoin1PropertyDict.put("foaf:givenName");
        Dictionary expectedJoin1ObjectDict = new Dictionary();
        expectedJoin1ObjectDict.put("LUKE");
        expectedJoin1ObjectDict.put("HAN");
        expectedJoin1ObjectDict.put("LEA");
        ComplexTable expectedJoin1Table = new ComplexTable(expectedJoin1PropertyDict, expectedJoin1ObjectDict);

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(0, joinedValue1, true));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(2, joinedValue2, true));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(24, joinedValue3, true));

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.sortMergeJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        Dictionary expectedJoin2PropertyDict = new Dictionary();
        expectedJoin2PropertyDict.put("wsdbm:userId");
        expectedJoin2PropertyDict.put("foaf:givenName");
        expectedJoin2PropertyDict.put("foaf:familyName");
        Dictionary expectedJoin2ObjectDict = new Dictionary();
        expectedJoin2ObjectDict.put("LUKE");
        expectedJoin2ObjectDict.put("HAN");
        expectedJoin2ObjectDict.put("LEA");
        expectedJoin2ObjectDict.put("SKYWALKER");
        expectedJoin2ObjectDict.put("SOLO");
        expectedJoin2ObjectDict.put("ORGANA");
        ComplexTable expectedJoin2Table = new ComplexTable(expectedJoin2PropertyDict,
                expectedJoin2ObjectDict);

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(2, joinedValue2, true));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        joinedValue3.put(3, new Item(24, 6, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(24, joinedValue3, true));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        Dictionary expectedJoin3PropertyDict = new Dictionary();
        expectedJoin3PropertyDict.put("wsdbm:userId");
        expectedJoin3PropertyDict.put("foaf:givenName");
        expectedJoin3PropertyDict.put("foaf:familyName");
        expectedJoin3PropertyDict.put("wsdbm:follows");
        ComplexTable expectedJoin3Table = new ComplexTable(
                expectedJoin3PropertyDict,
                expectedJoin2Table.getObjectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 4, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue2, true));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 5, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(2, joinedValue3, true));

        // subject 24 does not follow anyone, so remove givenName LEA and familyName ORGANA from the expected result dict
        expectedJoin3Table.getObjectDictionary().getValues().remove(3);
        expectedJoin3Table.getObjectDictionary().getInvertedValues().remove("LEA");
        expectedJoin3Table.getObjectDictionary().getValues().remove(6);
        expectedJoin3Table.getObjectDictionary().getInvertedValues().remove("ORGANA");

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoin3Table, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        Dictionary expectedJoin4PropertyDict = new Dictionary();
        expectedJoin4PropertyDict.put("wsdbm:userId");
        expectedJoin4PropertyDict.put("foaf:givenName");
        expectedJoin4PropertyDict.put("foaf:familyName");
        expectedJoin4PropertyDict.put("wsdbm:follows");
        expectedJoin4PropertyDict.put("wsdbm:likes");
        ComplexTable expectedJoin4Table = new ComplexTable(
                expectedJoin4PropertyDict,
                expectedJoin3Table.getObjectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        joinedValue1.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(0, joinedValue1, true));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        joinedValue2.put(4, new Item(2, 24, DataType.OBJECT));
        joinedValue2.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(2, joinedValue2, true));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameFamilyNameFollowsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoin4Table, actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable);
    }

    /**
     * Test parallel hash join of 2 multi-properties tables
     */
    @Test
    public void testComplexSortMergeJoin() {
        Database database = new Database(initSimpleTables());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        Dictionary expectedJoin1PropertyDict = new Dictionary();
        expectedJoin1PropertyDict.put("wsdbm:userId");
        expectedJoin1PropertyDict.put("foaf:givenName");
        expectedJoin1PropertyDict.put("foaf:familyName");
        expectedJoin1PropertyDict.put("wsdbm:follows");
        Dictionary expectedJoin1ObjectDict = new Dictionary();
        expectedJoin1ObjectDict.put("LUKE");
        expectedJoin1ObjectDict.put("HAN");
        expectedJoin1ObjectDict.put("SKYWALKER");
        expectedJoin1ObjectDict.put("SOLO");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(expectedJoin1PropertyDict,
                expectedJoin1ObjectDict);

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 3, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1, true));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 3, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2, true));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 4, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3, true));

        ComplexTable userIdGivenNameTable = joinService.sortMergeJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        ComplexTable userIdGivenNameFamilyNameTable = joinService.sortMergeJoin(
                userIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        ComplexTable userIdGivenNameFamilyNameFollowsTable = joinService.sortMergeJoin(
                userIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.sortMergeJoin(
                userIdGivenNameFamilyNameTable,
                userIdGivenNameFamilyNameFollowsTable,
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:userId",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsTable, actualJoinedUserIdGivenNameFamilyNameFollowsTable);
    }

    /**
     * Test sequence parallel hash join of 1 property tables
     */
    @Test
    public void testSimpleParallelOptimizedJoin() {
        Database database = new Database(initSimpleTables());

        // join userId on givenName

        HashMap<String, Dictionary> expectedDict = new HashMap<>();
        expectedDict.put("wsdbm:userId", database.tables().get("wsdbm:userId").getObjectDictionary());
        expectedDict.put("foaf:givenName", database.tables().get("foaf:givenName").getObjectDictionary());

        HashMap<String, PropertyValues<Item>> expectedValues1 = new HashMap<>();
        expectedValues1.put("wsdbm:userId", new PropertyValues<>(
                List.of(
                        new Item(0, 1806723, DataType.INTEGER),
                        new Item(2, 1936247, DataType.INTEGER),
                        new Item(24, 15125125, DataType.INTEGER))));
        expectedValues1.put("foaf:givenName", new PropertyValues<>(
                List.of(
                        new Item(0, 1, DataType.STRING),
                        new Item(2, 2, DataType.STRING),
                        new Item(24, 3, DataType.STRING))));

        VerticallyPartitionedTable expectedJoin1Table = new VerticallyPartitionedTable(expectedDict, expectedValues1);

        VerticallyPartitionedTable actualJoinedUserIdGivenNameTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:userId").toVerticallyPartitioned(),
                database.tables().get("foaf:givenName").toVerticallyPartitioned(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        expectedDict.put("foaf:familyName", database.tables().get("foaf:familyName").getObjectDictionary());

        var expectedValues2 = (HashMap<String, PropertyValues<Item>>) expectedValues1.clone();
        expectedValues2.put("foaf:familyName", new PropertyValues<>(
                List.of(
                        new Item(0, 1, DataType.STRING),
                        new Item(2, 2, DataType.STRING),
                        new Item(24, 3, DataType.STRING))));

        VerticallyPartitionedTable expectedJoin2Table = new VerticallyPartitionedTable(expectedDict, expectedValues2);

        VerticallyPartitionedTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toVerticallyPartitioned(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        expectedDict.put("wsdbm:follows", database.tables().get("wsdbm:follows").getObjectDictionary());

        HashMap<String, PropertyValues<Item>> expectedValues3 = new HashMap<>();
        expectedValues3.put("wsdbm:userId", new PropertyValues<>(
                List.of(
                        new Item(0, 1806723, DataType.INTEGER),
                        new Item(0, 1806723, DataType.INTEGER),
                        new Item(2, 1936247, DataType.INTEGER))));
        expectedValues3.put("foaf:givenName", new PropertyValues<>(
                List.of(
                        new Item(0, 1, DataType.STRING),
                        new Item(0, 1, DataType.STRING),
                        new Item(2, 2, DataType.STRING))));
        expectedValues3.put("foaf:familyName", new PropertyValues<>(
                List.of(
                        new Item(0, 1, DataType.STRING),
                        new Item(0, 1, DataType.STRING),
                        new Item(2, 2, DataType.STRING))));
        expectedValues3.put("wsdbm:follows", new PropertyValues<>(
                List.of(
                        new Item(0, 24, DataType.OBJECT),
                        new Item(0, 27, DataType.OBJECT),
                        new Item(2, 24, DataType.OBJECT))));

        VerticallyPartitionedTable expectedJoin3Table = new VerticallyPartitionedTable(expectedDict, expectedValues3);

        VerticallyPartitionedTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toVerticallyPartitioned(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoin3Table, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        expectedDict.put("wsdbm:likes", database.tables().get("wsdbm:likes").getObjectDictionary());

        HashMap<String, PropertyValues<Item>> expectedValues4 = new HashMap<>();
        expectedValues4.put("wsdbm:userId", new PropertyValues<>(
                List.of(
                        new Item(0, 1806723, DataType.INTEGER),
                        new Item(2, 1936247, DataType.INTEGER))));
        expectedValues4.put("foaf:givenName", new PropertyValues<>(
                List.of(
                        new Item(0, 1, DataType.STRING),
                        new Item(2, 2, DataType.STRING))));
        expectedValues4.put("foaf:familyName", new PropertyValues<>(
                List.of(
                        new Item(0, 1, DataType.STRING),
                        new Item(2, 2, DataType.STRING))));
        expectedValues4.put("wsdbm:follows", new PropertyValues<>(
                List.of(
                        new Item(0, 24, DataType.OBJECT),
                        new Item(2, 24, DataType.OBJECT))));

        expectedValues4.put("wsdbm:likes", new PropertyValues<>(
                List.of(
                        new Item(24, 25, DataType.OBJECT),
                        new Item(24, 25, DataType.OBJECT))));

        VerticallyPartitionedTable expectedJoin4Table = new VerticallyPartitionedTable(expectedDict, expectedValues4);

        VerticallyPartitionedTable actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameFamilyNameFollowsTable,
                database.tables().get("wsdbm:likes").toVerticallyPartitioned(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoin4Table, actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable);

    }

    private void compareTables(ComplexTable expected, ComplexTable actual) {
        // check sizes
        Assert.assertEquals(String.format("Joined Table size should be %d, got %d", expected.getValues().size(), actual.getValues().size()),
                expected.getValues().size(), actual.getValues().size());

        // check properties
        assertDictionariesEqual(expected.getPropertyDictionary(), actual.getPropertyDictionary());

        // check dictionaries
        assertDictionariesEqual(expected.getObjectDictionary(), actual.getObjectDictionary());

        // check tuples
        assertJoinedValuesEqual(expected.getValues(), actual.getValues(), expected, actual);
    }

    private void compareTables(VerticallyPartitionedTable expected, VerticallyPartitionedTable actual) {
        // check sizes
        Assert.assertEquals(String.format("Joined Table size should be %d, got %d", expected.size(), actual.size()),
                expected.size(), actual.size());

        // check dictionaries
        Assert.assertEquals("Dictionaries should contain the same references", expected.dictionaries(), actual.dictionaries());

        // check joined items
        assertJoinedValuesEqual(expected.itemsToHorizontalPartition(), actual.itemsToHorizontalPartition());

    }

    private void assertJoinedValuesEqual(List<JoinedItemsOptimized> j1, List<JoinedItemsOptimized> j2) {
        Assert.assertEquals("Joined Table should have the same length (" + j1.size() + " vs. " + j2.size() + ")", j1.size(), j2.size());

        // For each JoinedItems in j1
        for (JoinedItemsOptimized joinedItems1 : j1) {
            // Find it in j2
            boolean found = false;
            for (JoinedItemsOptimized joinedItems2 : j2) {
                if (joinedItems1.equals(joinedItems2)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                System.out.println("Expected: " + j1);
                System.out.println("Actual: " + j2);
                Assert.fail("Joined Table should have correctly joined values with correct structure");
            }
        }

    }

    private void assertDictionariesEqual(Dictionary d1, Dictionary d2) {
        if (d1.getValues().size() != d2.getValues().size()) {
            System.out.println("Expected: " + d1);
            System.out.println("Actual: " + d2);
            Assert.fail("Dictionaries should have the same length");
        }

        for (String d1Key : d1.getInvertedValues().keySet()) {
            // Find d1Key in d2
            if (!d2.getInvertedValues().containsKey(d1Key)) {
                System.out.println("Expected: " + d1);
                System.out.println("Actual: " + d2);
                Assert.fail("Key missing in dictionary");
            }
        }
    }

    private void assertJoinedValuesEqual(List<JoinedItems> j1, List<JoinedItems> j2, ComplexTable t1, ComplexTable t2) {
        Assert.assertEquals("Joined Table should have the same length (" + j1.size() + " vs. " + j2.size() + ")", j1.size(), j2.size());

        Dictionary d1 = t1.getObjectDictionary();
        Dictionary p1 = t1.getPropertyDictionary();
        Dictionary d2 = t2.getObjectDictionary();
        Dictionary p2 = t2.getPropertyDictionary();
        // For each JoinedItems in j1
        for (JoinedItems joinedItems1 : j1) {
            // Find it in j2
            boolean found = false;
            for (JoinedItems joinedItems2 : j2) {
                JoinedItems tempJoinedItem2 = joinedItems2.clone();
                // Switch dictionaries and properties in tempJoinedItem2 so comparing becomes very easy
                for (var property : joinedItems2.values().keySet()) {
                    Item item = joinedItems2.values().get(property);
                    String propertyValue = p2.get(property);
                    int commonPropertyInt = p1.getInvertedValues().get(propertyValue);
                    int objectInt = item.object();
                    if (item.type() == DataType.STRING) {
                        String value2 = d2.get(item.object());
                        if (d1.getInvertedValues().containsKey(value2)) {
                            objectInt = d1.getInvertedValues().get(value2);
                        }
                    }
                    tempJoinedItem2.values().put(commonPropertyInt,
                            new Item(item.subject(),
                                    objectInt,
                                    item.type()));
                }
                if (joinedItems1.equals(tempJoinedItem2)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                System.out.println("Expected: " + j1);
                System.out.println("using properties: " + p1);
                System.out.println("using dictionary: " + d1);
                System.out.println("Actual: " + j2);
                System.out.println("using properties: " + p2);
                System.out.println("using dictionary: " + d2);
                Assert.fail("Joined Table should have correctly joined values with correct structure");
            }
        }
    }

    private HashMap<String, SimpleTable> initSimpleTables() {
        HashMap<String, SimpleTable> tables = new HashMap<>();

        Dictionary givenNameDict = new Dictionary();
        givenNameDict.put("LUKE");
        givenNameDict.put("HAN");
        givenNameDict.put("LEA");
        SimpleTable givenNameTable = new SimpleTable("foaf:givenName", givenNameDict);
        givenNameTable.insert(new Item(0, 1, DataType.STRING));
        givenNameTable.insert(new Item(2, 2, DataType.STRING));
        givenNameTable.insert(new Item(24, 3, DataType.STRING));
        tables.put("foaf:givenName", givenNameTable);

        Dictionary familyNameDict = new Dictionary();
        familyNameDict.put("SKYWALKER");
        familyNameDict.put("SOLO");
        familyNameDict.put("ORGANA");
        SimpleTable familyNameTable = new SimpleTable("foaf:familyName", familyNameDict);
        familyNameTable.insert(new Item(0, 1, DataType.STRING));
        familyNameTable.insert(new Item(2, 2, DataType.STRING));
        familyNameTable.insert(new Item(24, 3, DataType.STRING));
        tables.put("foaf:familyName", familyNameTable);

        SimpleTable userIdTable = new SimpleTable("wsdbm:userId");
        userIdTable.insert(new Item(0, 1806723, DataType.INTEGER));
        userIdTable.insert(new Item(2, 1936247, DataType.INTEGER));
        userIdTable.insert(new Item(24, 15125125, DataType.INTEGER));
        tables.put("wsdbm:userId", userIdTable);

        SimpleTable followsTable = new SimpleTable("wsdbm:follows");
        followsTable.insert(new Item(0, 24, DataType.OBJECT));
        followsTable.insert(new Item(0, 27, DataType.OBJECT));
        followsTable.insert(new Item(2, 24, DataType.OBJECT));
        tables.put("wsdbm:follows", followsTable);

        SimpleTable likesTable = new SimpleTable("wsdbm:likes");
        likesTable.insert(new Item(24, 25, DataType.OBJECT));
        likesTable.insert(new Item(29, 30, DataType.OBJECT));
        tables.put("wsdbm:likes", likesTable);

        return tables;
    }
}