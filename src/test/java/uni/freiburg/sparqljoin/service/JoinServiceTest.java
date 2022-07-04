package uni.freiburg.sparqljoin.service;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import uni.freiburg.sparqljoin.join.JoinOn;
import uni.freiburg.sparqljoin.model.db.*;
import uni.freiburg.sparqljoin.model.db.Dictionary;
import uni.freiburg.sparqljoin.model.join.JoinedItems;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class JoinServiceTest {

    @InjectMocks
    JoinService joinService;

    /**
     * Test sequence hash join of 1 property tables
     */
    @Test
    public void testSimpleHashJoin() {
        Database database = new Database(initTables());

        // join userId on givenName

        Dictionary expectedJoinedUserIdGivenNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameDict.put("HAN");
        expectedJoinedUserIdGivenNameDict.put("LEA");
        ComplexTable expectedJoinedUserIdGivenNameTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName")), expectedJoinedUserIdGivenNameDict);

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

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.hashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameTable, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        Dictionary expectedJoinedUserIdGivenNameFamilyNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("HAN");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LEA");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SKYWALKER");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SOLO");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("ORGANA");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName")),
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

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.hashJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameTable, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(
                new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows")),
                expectedJoinedUserIdGivenNameFamilyNameTable.getDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        joinedValue1.put("wsdbm:follows", new Item<>(0, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue2.put("foaf:givenName", new Item<>(0, 1));
        joinedValue2.put("foaf:familyName", new Item<>(0, 4));
        joinedValue2.put("wsdbm:follows", new Item<>(0, 27));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue3.put("foaf:givenName", new Item<>(2, 2));
        joinedValue3.put("foaf:familyName", new Item<>(2, 5));
        joinedValue3.put("wsdbm:follows", new Item<>(2, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.hashJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsTable, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable = new ComplexTable(
                new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows", "wsdbm:likes")),
                expectedJoinedUserIdGivenNameFamilyNameFollowsTable.getDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        joinedValue1.put("wsdbm:follows", new Item<>(0, 24));
        joinedValue1.put("wsdbm:likes", new Item<>(24, 25));
        expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue2.put("foaf:givenName", new Item<>(2, 2));
        joinedValue2.put("foaf:familyName", new Item<>(2, 5));
        joinedValue2.put("wsdbm:follows", new Item<>(2, 24));
        joinedValue2.put("wsdbm:likes", new Item<>(24, 25));
        expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable.insert(new JoinedItems(2, joinedValue2));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable = joinService.hashJoin(
                actualJoinedUserIdGivenNameFamilyNameFollowsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable, actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable);
    }

    /**
     * Test hash join of 2 multi-properties tables
     */
    @Test
    public void testComplexHashJoin() {
        Database database = new Database(initTables());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        Dictionary expectedJoinedUserIdGivenNameFamilyNameFollowsDict = new Dictionary();
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("LUKE");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("HAN");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("LEA");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("SKYWALKER");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("SOLO");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("ORGANA");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows")),
                expectedJoinedUserIdGivenNameFamilyNameFollowsDict);

        HashMap<String, Item<Integer>> joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        HashMap<String, Item<Integer>> joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue2.put("foaf:givenName", new Item<>(0, 1));
        joinedValue2.put("foaf:familyName", new Item<>(0, 4));
        joinedValue2.put("wsdbm:follows", new Item<>(0, 27));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue2));
        HashMap<String, Item<Integer>> joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue3.put("foaf:givenName", new Item<>(2, 2));
        joinedValue3.put("foaf:familyName", new Item<>(2, 5));
        joinedValue3.put("wsdbm:follows", new Item<>(2, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(24, joinedValue3));

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
        Database database = new Database(initTables());

        // join userId on givenName

        Dictionary expectedJoinedUserIdGivenNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameDict.put("HAN");
        expectedJoinedUserIdGivenNameDict.put("LEA");
        ComplexTable expectedJoinedUserIdGivenNameTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName")), expectedJoinedUserIdGivenNameDict);

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

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameTable, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        Dictionary expectedJoinedUserIdGivenNameFamilyNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("HAN");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LEA");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SKYWALKER");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SOLO");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("ORGANA");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName")),
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

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameTable, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(
                new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows")),
                expectedJoinedUserIdGivenNameFamilyNameTable.getDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        joinedValue1.put("wsdbm:follows", new Item<>(0, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue2.put("foaf:givenName", new Item<>(0, 1));
        joinedValue2.put("foaf:familyName", new Item<>(0, 4));
        joinedValue2.put("wsdbm:follows", new Item<>(0, 27));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue3.put("foaf:givenName", new Item<>(2, 2));
        joinedValue3.put("foaf:familyName", new Item<>(2, 5));
        joinedValue3.put("wsdbm:follows", new Item<>(2, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsTable, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable = new ComplexTable(
                new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows", "wsdbm:likes")),
                expectedJoinedUserIdGivenNameFamilyNameFollowsTable.getDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        joinedValue1.put("wsdbm:follows", new Item<>(0, 24));
        joinedValue1.put("wsdbm:likes", new Item<>(24, 25));
        expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue2.put("foaf:givenName", new Item<>(2, 2));
        joinedValue2.put("foaf:familyName", new Item<>(2, 5));
        joinedValue2.put("wsdbm:follows", new Item<>(2, 24));
        joinedValue2.put("wsdbm:likes", new Item<>(24, 25));
        expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable.insert(new JoinedItems(2, joinedValue2));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable = joinService.parallelHashJoin(
                actualJoinedUserIdGivenNameFamilyNameFollowsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable, actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable);
    }

    /**
     * Test parallel hash join of 2 multi-properties tables
     */
    @Test
    public void testComplexParallelHashJoin() {
        Database database = new Database(initTables());

        // join userId, givenName, familyName on userId, givenName, familyName, follows

        Dictionary expectedJoinedUserIdGivenNameFamilyNameFollowsDict = new Dictionary();
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("LUKE");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("HAN");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("LEA");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("SKYWALKER");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("SOLO");
        expectedJoinedUserIdGivenNameFamilyNameFollowsDict.put("ORGANA");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows")),
                expectedJoinedUserIdGivenNameFamilyNameFollowsDict);

        HashMap<String, Item<Integer>> joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        HashMap<String, Item<Integer>> joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue2.put("foaf:givenName", new Item<>(0, 1));
        joinedValue2.put("foaf:familyName", new Item<>(0, 4));
        joinedValue2.put("wsdbm:follows", new Item<>(0, 27));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue2));
        HashMap<String, Item<Integer>> joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue3.put("foaf:givenName", new Item<>(2, 2));
        joinedValue3.put("foaf:familyName", new Item<>(2, 5));
        joinedValue3.put("wsdbm:follows", new Item<>(2, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(24, joinedValue3));

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
        Database database = new Database(initTables());

        // join userId on givenName

        Dictionary expectedJoinedUserIdGivenNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameDict.put("HAN");
        expectedJoinedUserIdGivenNameDict.put("LEA");
        ComplexTable expectedJoinedUserIdGivenNameTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName")), expectedJoinedUserIdGivenNameDict);

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

        ComplexTable actualJoinedUserIdGivenNameTable = joinService.sortMergeJoin(
                database.tables().get("wsdbm:userId").toComplex(),
                database.tables().get("foaf:givenName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:givenName",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameTable, actualJoinedUserIdGivenNameTable);

        // join userId, givenName on familyName

        Dictionary expectedJoinedUserIdGivenNameFamilyNameDict = new Dictionary();
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LUKE");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("HAN");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("LEA");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SKYWALKER");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("SOLO");
        expectedJoinedUserIdGivenNameFamilyNameDict.put("ORGANA");
        ComplexTable expectedJoinedUserIdGivenNameFamilyNameTable = new ComplexTable(new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName")),
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

        ComplexTable actualJoinedUserIdGivenNameFamilyNameTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameTable,
                database.tables().get("foaf:familyName").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "foaf:familyName",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameTable, actualJoinedUserIdGivenNameFamilyNameTable);

        // join userId, givenName, familyName on follows

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsTable = new ComplexTable(
                new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows")),
                expectedJoinedUserIdGivenNameFamilyNameTable.getDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        joinedValue1.put("wsdbm:follows", new Item<>(0, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue2.put("foaf:givenName", new Item<>(0, 1));
        joinedValue2.put("foaf:familyName", new Item<>(0, 4));
        joinedValue2.put("wsdbm:follows", new Item<>(0, 27));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(0, joinedValue2));
        joinedValue3 = new HashMap<>();
        joinedValue3.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue3.put("foaf:givenName", new Item<>(2, 2));
        joinedValue3.put("foaf:familyName", new Item<>(2, 5));
        joinedValue3.put("wsdbm:follows", new Item<>(2, 24));
        expectedJoinedUserIdGivenNameFamilyNameFollowsTable.insert(new JoinedItems(2, joinedValue3));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameFamilyNameTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:userId",
                JoinOn.SUBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsTable, actualJoinedUserIdGivenNameFamilyNameFollowsTable);

        // join userId, givenName, familyName, follows on likes

        ComplexTable expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable = new ComplexTable(
                new LinkedHashSet<>(List.of("wsdbm:userId", "foaf:givenName", "foaf:familyName", "wsdbm:follows", "wsdbm:likes")),
                expectedJoinedUserIdGivenNameFamilyNameFollowsTable.getDictionary());

        joinedValue1 = new HashMap<>();
        joinedValue1.put("wsdbm:userId", new Item<>(0, 1806723));
        joinedValue1.put("foaf:givenName", new Item<>(0, 1));
        joinedValue1.put("foaf:familyName", new Item<>(0, 4));
        joinedValue1.put("wsdbm:follows", new Item<>(0, 24));
        joinedValue1.put("wsdbm:likes", new Item<>(24, 25));
        expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable.insert(new JoinedItems(0, joinedValue1));
        joinedValue2 = new HashMap<>();
        joinedValue2.put("wsdbm:userId", new Item<>(2, 1936247));
        joinedValue2.put("foaf:givenName", new Item<>(2, 2));
        joinedValue2.put("foaf:familyName", new Item<>(2, 5));
        joinedValue2.put("wsdbm:follows", new Item<>(2, 24));
        joinedValue2.put("wsdbm:likes", new Item<>(24, 25));
        expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable.insert(new JoinedItems(2, joinedValue2));

        ComplexTable actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable = joinService.sortMergeJoin(
                actualJoinedUserIdGivenNameFamilyNameFollowsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);

        compareTables(expectedJoinedUserIdGivenNameFamilyNameFollowsLikesTable, actualJoinedUserIdGivenNameFamilyNameFollowsLikesTable);
    }

    private void compareTables(ComplexTable expected, ComplexTable actual) {
        // check sizes
        Assert.assertEquals(String.format("Joined Table size should be %d, got %d", expected.getValues().size(), actual.getValues().size()),
                expected.getValues().size(), actual.getValues().size());
        // check properties
        Assert.assertEquals(String.format("Joined Table should have properties '%s', got '%s'", expected.getProperties(), actual.getProperties()),
                expected.getProperties(), actual.getProperties());
        // check dictionaries
        var expectedDictValues = expected.getDictionary();
        var actualDictValues = actual.getDictionary();
        assertDictionariesEqual(expectedDictValues, actualDictValues);
        assertJoinedValuesEqual(expected.getValues(), actual.getValues(), expectedDictValues, actualDictValues);
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

    private void assertJoinedValuesEqual(List<JoinedItems> j1, List<JoinedItems> j2, Dictionary d1, Dictionary d2) {
        Assert.assertEquals("Joined Table should have the same length (" + j1.size() + " vs. " + j2.size() + ")", j1.size(), j2.size());

        for (JoinedItems joinedItems1 : j1) {
            // Find it in j2
            boolean found = false;
            for (JoinedItems joinedItems2 : j2) {
                JoinedItems tempJoinedItem2 = joinedItems2.clone();
                for (var property : tempJoinedItem2.values().keySet()) {
                    if (d2.containsKey(tempJoinedItem2.values().get(property).object())) {
                        // Value of the property was a string
                        String value2 = d2.get(tempJoinedItem2.values().get(property).object());
                        if (d1.getInvertedValues().containsKey(value2)) {
                            tempJoinedItem2.values().put(property, new Item<>(tempJoinedItem2.subject(), d1.getInvertedValues().get(value2).intValue()));
                        }
                    }
                }
                if (joinedItems1.equals(tempJoinedItem2)) {
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