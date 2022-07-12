package uni.freiburg.sparqljoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uni.freiburg.sparqljoin.join.JoinOn;
import uni.freiburg.sparqljoin.model.db.ComplexTable;
import uni.freiburg.sparqljoin.model.db.Database;
import uni.freiburg.sparqljoin.model.db.VerticallyPartitionedTable;
import uni.freiburg.sparqljoin.service.DataLoaderService;
import uni.freiburg.sparqljoin.service.JoinService;
import uni.freiburg.sparqljoin.util.Performance;

@SpringBootApplication
public class SparqlJoinApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparqlJoinApplication.class);

    @Autowired
    DataLoaderService dataLoaderService;

    @Autowired
    JoinService joinService;

    @Value("${datasetPath}")
    private String datasetPath;

    public static void main(String[] args) {
        LOG.debug("Starting SPARQL join algorithms");
        SpringApplication app = new SpringApplication(SparqlJoinApplication.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        Performance.measure(this::perform, "Overall Run");
        LOG.info("SPARQL finished successfully");
    }

    public boolean perform() {
        // a) load data, build dictionaries
        Database database = Performance.measure(this::loadData, "Load Data");
        // b), c) join algorithms simulations
        Performance.measure(() -> this.simulation(database), "Join Simulation");
        return true;
    }

    public Database loadData() {
        return dataLoaderService.load(datasetPath);
    }

    public boolean simulation(Database database) {
        LOG.info("Simulation begin...");

        // hash join optimized
        LOG.info("****** HASH JOIN OPTIMIZED ******");
        Performance.measure(() -> hashJoinOptimized(database), "Hash Join Optimized Simulation");

        LOG.info("****** PARALLEL JOIN OPTIMIZED ******");
        Performance.measure(() -> parallelHashJoinOptimized(database), "Parallel Hash Join Optimized Simulation");

        // hash join
        LOG.info("****** HASH JOIN ******");
        Performance.measure(() -> hashJoin(database), "Hash Join Simulation");

        // sort merge simulation
        LOG.info("****** SORT-MERGE JOIN ******");
        Performance.measure(() -> sortMergeJoin(database), "Sort-Merge Join Simulation");

        // parallel hash join
        LOG.info("****** PARALLEL JOIN ******");
        Performance.measure(() -> parallelHashJoin(database), "Parallel Hash Join Simulation");

        return true;
    }

    public ComplexTable hashJoin(Database database) {
        ComplexTable likesHasReviewTable = joinService.hashJoin(
                database.tables().get("wsdbm:likes").toComplex(),
                database.tables().get("rev:hasReview").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        ComplexTable likesHasReviewFollowsTable = joinService.hashJoin(
                likesHasReviewTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);
        ComplexTable joinedTable = joinService.hashJoin(
                likesHasReviewFollowsTable,
                database.tables().get("wsdbm:friendOf").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        LOG.info("Hash joined table size: {}", joinedTable.getValues().size());
        return joinedTable;
    }

    public VerticallyPartitionedTable hashJoinOptimized(Database database) {
        VerticallyPartitionedTable likesHasReviewTable = joinService.hashJoin(
                database.tables().get("wsdbm:likes").toVerticallyPartitioned(),
                database.tables().get("rev:hasReview").toVerticallyPartitioned(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        VerticallyPartitionedTable likesHasReviewFollowsTable = joinService.hashJoin(
                likesHasReviewTable,
                database.tables().get("wsdbm:follows").toVerticallyPartitioned(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);
        VerticallyPartitionedTable joinedTable = joinService.hashJoin(
                likesHasReviewFollowsTable,
                database.tables().get("wsdbm:friendOf").toVerticallyPartitioned(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        LOG.info("Hash joined table size: {}", joinedTable.propertyItems().get("wsdbm:likes").getValues().size());
        return joinedTable;
    }

    public ComplexTable parallelHashJoin(Database database) {
        ComplexTable likesHasReviewTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:likes").toComplex(),
                database.tables().get("rev:hasReview").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        ComplexTable likesHasReviewFollowsTable = joinService.parallelHashJoin(
                likesHasReviewTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);
        ComplexTable joinedTable = joinService.parallelHashJoin(
                likesHasReviewFollowsTable,
                database.tables().get("wsdbm:friendOf").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        LOG.info("Hash joined table size: {}", joinedTable.getValues().size());
        return joinedTable;
    }

    public VerticallyPartitionedTable parallelHashJoinOptimized(Database database) {
        VerticallyPartitionedTable likesHasReviewTable = joinService.parallelHashJoin(
                database.tables().get("wsdbm:likes").toVerticallyPartitioned(),
                database.tables().get("rev:hasReview").toVerticallyPartitioned(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        VerticallyPartitionedTable likesHasReviewFollowsTable = joinService.parallelHashJoin(
                likesHasReviewTable,
                database.tables().get("wsdbm:follows").toVerticallyPartitioned(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);
        VerticallyPartitionedTable joinedTable = joinService.parallelHashJoin(
                likesHasReviewFollowsTable,
                database.tables().get("wsdbm:friendOf").toVerticallyPartitioned(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        LOG.info("Hash joined table size: {}", joinedTable.size());
        return joinedTable;
    }

    public ComplexTable sortMergeJoin(Database database) {
        ComplexTable likesHasReviewTable = joinService.sortMergeJoin(
                database.tables().get("wsdbm:likes").toComplex(),
                database.tables().get("rev:hasReview").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        ComplexTable likesHasReviewFollowsTable = joinService.sortMergeJoin(
                likesHasReviewTable,
                database.tables().get("wsdbm:follows").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "wsdbm:follows",
                JoinOn.SUBJECT);
        ComplexTable joinedTable = joinService.sortMergeJoin(
                likesHasReviewFollowsTable,
                database.tables().get("wsdbm:friendOf").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        LOG.info("Sort-Merge joined table size: {}", joinedTable.getValues().size());
        return joinedTable;
    }
}
