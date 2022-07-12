package uni.freiburg.sparqljoin.service;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import uni.freiburg.sparqljoin.join.JoinOn;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.join.JoinedItems;

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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database database = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        // join userId on givenName

        ComplexTable expectedJoin1Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(0, joinedValue1));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(2, joinedValue2));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoin1Table = joinService.hashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoin1Table);

        // join userId, givenName on familyName

        ComplexTable expectedJoin2Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(2, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        joinedValue3.put(3, new Item(24, 6, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoin2Table = joinService.hashJoin(
                actualJoin1Table,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoin2Table);

        // join userId, givenName, familyName on follows

        ComplexTable expectedJoin3Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 4, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 5, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(2, joinedValue3));

        ComplexTable actualJoin3Table = joinService.hashJoin(
                actualJoin2Table,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoin3Table, actualJoin3Table);

        // join userId, givenName, familyName, follows on likes

        ComplexTable expectedJoin4Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        joinedValue1.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        joinedValue2.put(4, new Item(2, 24, DataType.OBJECT));
        joinedValue2.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(2, joinedValue2));

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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database database = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 3, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 3, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 4, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3));

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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database database = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        // join userId on givenName

        ComplexTable expectedJoin1Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(0, joinedValue1));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(2, joinedValue2));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        ComplexTable expectedJoin2Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(2, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        joinedValue3.put(3, new Item(24, 6, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        ComplexTable expectedJoin3Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 4, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 5, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(2, joinedValue3));

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

        ComplexTable expectedJoin4Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        joinedValue1.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        joinedValue2.put(4, new Item(2, 24, DataType.OBJECT));
        joinedValue2.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(2, joinedValue2));

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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database database = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 3, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 3, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 4, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3));

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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database database = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        // join userId on givenName

        ComplexTable expectedJoin1Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(0, joinedValue1));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(2, joinedValue2));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        expectedJoin1Table.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.sortMergeJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin1Table, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        ComplexTable expectedJoin2Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(2, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(24, 15125125, DataType.INTEGER));
        joinedValue3.put(2, new Item(24, 3, DataType.STRING));
        joinedValue3.put(3, new Item(24, 6, DataType.STRING));
        expectedJoin2Table.insert(new JoinedItems(24, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoin2Table, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        ComplexTable expectedJoin3Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 4, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(0, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 5, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoin3Table.insert(new JoinedItems(2, joinedValue3));

        // subject 24 does not follow anyone, so remove givenName LEA and familyName ORGANA from the expected result dict

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoin3Table, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        ComplexTable expectedJoin4Table = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 4, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        joinedValue1.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue2.put(2, new Item(2, 2, DataType.STRING));
        joinedValue2.put(3, new Item(2, 5, DataType.STRING));
        joinedValue2.put(4, new Item(2, 24, DataType.OBJECT));
        joinedValue2.put(5, new Item(24, 25, DataType.OBJECT));
        expectedJoin4Table.insert(new JoinedItems(2, joinedValue2));

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
        PreparedDataset expectedDataset = initExpectedDataset();
        Database database = new Database(expectedDataset.simpleTables(), expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(expectedDataset.propertyDictionary(), expectedDataset.objectDictionary());

        HashMap<Integer, Item> joinedValue1 = new HashMap<>();
        joinedValue1.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue1.put(2, new Item(0, 1, DataType.STRING));
        joinedValue1.put(3, new Item(0, 3, DataType.STRING));
        joinedValue1.put(4, new Item(0, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        HashMap<Integer, Item> joinedValue2 = new HashMap<>();
        joinedValue2.put(1, new Item(0, 1806723, DataType.INTEGER));
        joinedValue2.put(2, new Item(0, 1, DataType.STRING));
        joinedValue2.put(3, new Item(0, 3, DataType.STRING));
        joinedValue2.put(4, new Item(0, 27, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2));
        HashMap<Integer, Item> joinedValue3 = new HashMap<>();
        joinedValue3.put(1, new Item(2, 1936247, DataType.INTEGER));
        joinedValue3.put(2, new Item(2, 2, DataType.STRING));
        joinedValue3.put(3, new Item(2, 4, DataType.STRING));
        joinedValue3.put(4, new Item(2, 24, DataType.OBJECT));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3));

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
                        objectInt = d1.getInvertedValues().get(value2);
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
                System.out.println("Did not find a match for: " + j1);
                Assert.fail("Joined Table should have correctly joined values with correct structure");
            }
        }
    }

    private Dictionary initExpectedObjectDictionary() {
        Dictionary dictionary = new Dictionary();
        dictionary.put("LUKE");
        dictionary.put("HAN");
        dictionary.put("LEA");
        dictionary.put("SKYWALKER");
        dictionary.put("SOLO");
        dictionary.put("ORGANA");
        return dictionary;
    }

    private Dictionary initExpectedPropertyDictionary() {
        Dictionary propertyDictionary = new Dictionary();
        propertyDictionary.put("foaf:givenName");
        propertyDictionary.put("foaf:familyName");
        propertyDictionary.put("wsdbm:userId");
        propertyDictionary.put("gn:parentCountry");
        propertyDictionary.put("wsdbm:follows");
        propertyDictionary.put("wsdbm:likes");
        return propertyDictionary;
    }

    private PreparedDataset initExpectedDataset() {
        Dictionary propertyDictionary = initExpectedPropertyDictionary();
        Dictionary objectDictionary = initExpectedObjectDictionary();
        HashMap<String, SimpleTable> tables = new HashMap<>();

        SimpleTable givenNameTable = new SimpleTable("foaf:givenName", propertyDictionary, objectDictionary);
        givenNameTable.insert(new Item(0, 1, DataType.STRING));
        givenNameTable.insert(new Item(2, 2, DataType.STRING));
        givenNameTable.insert(new Item(24, 3, DataType.STRING));
        tables.put("foaf:givenName", givenNameTable);

        SimpleTable familyNameTable = new SimpleTable("foaf:familyName", propertyDictionary, objectDictionary);
        familyNameTable.insert(new Item(0, 4, DataType.STRING));
        familyNameTable.insert(new Item(2, 5, DataType.STRING));
        familyNameTable.insert(new Item(24, 6, DataType.STRING));
        tables.put("foaf:familyName", familyNameTable);

        SimpleTable userIdTable = new SimpleTable("wsdbm:userId", propertyDictionary, objectDictionary);
        userIdTable.insert(new Item(0, 1806723, DataType.INTEGER));
        userIdTable.insert(new Item(2, 1936247, DataType.INTEGER));
        userIdTable.insert(new Item(24, 15125125, DataType.INTEGER));
        tables.put("wsdbm:userId", userIdTable);

        SimpleTable followsTable = new SimpleTable("wsdbm:follows", propertyDictionary, objectDictionary);
        followsTable.insert(new Item(0, 24, DataType.OBJECT));
        followsTable.insert(new Item(0, 27, DataType.OBJECT));
        followsTable.insert(new Item(2, 24, DataType.OBJECT));
        tables.put("wsdbm:follows", followsTable);

        SimpleTable likesTable = new SimpleTable("wsdbm:likes", propertyDictionary, objectDictionary);
        likesTable.insert(new Item(24, 25, DataType.OBJECT));
        tables.put("wsdbm:likes", likesTable);

        return new PreparedDataset(tables, propertyDictionary, objectDictionary);
    }
}