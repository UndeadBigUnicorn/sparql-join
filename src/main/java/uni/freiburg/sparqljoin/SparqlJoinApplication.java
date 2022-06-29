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
        // hash join
        LOG.info("****** HASH JOIN ******");
        Performance.measure(() -> hashJoin(database), "Hash Join Simulation");
        // sort merge simulation
        LOG.info("****** SORT-MERGE JOIN ******");
        Performance.measure(() -> sortMergeJoin(database), "Sort-Merge Join Simulation");
        return true;
    }

    public ComplexTable hashJoin(Database database) {
        ComplexTable followsFriendsTable = joinService.hashJoin(
                database.tables().get("wsdbm:follows").toComplex(),
                database.tables().get("wsdbm:friendOf").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        ComplexTable followsFriendsLikesTable = joinService.hashJoin(
                followsFriendsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:friendOf",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);
        ComplexTable joinedTable = joinService.hashJoin(
                followsFriendsLikesTable,
                database.tables().get("rev:hasReview").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        LOG.info("Hash joined table size: {}", joinedTable.getValues().size());
        return joinedTable;
    }

    public ComplexTable sortMergeJoin(Database database) {
        ComplexTable followsFriendsTable = joinService.sortMergeJoin(
                database.tables().get("wsdbm:follows").toComplex(),
                database.tables().get("wsdbm:friendOf").toComplex(),
                "wsdbm:follows",
                JoinOn.OBJECT,
                "wsdbm:friendOf",
                JoinOn.SUBJECT);
        ComplexTable followsFriendsLikesTable = joinService.sortMergeJoin(
                followsFriendsTable,
                database.tables().get("wsdbm:likes").toComplex(),
                "wsdbm:friendOf",
                JoinOn.OBJECT,
                "wsdbm:likes",
                JoinOn.SUBJECT);
        ComplexTable joinedTable = joinService.sortMergeJoin(
                followsFriendsLikesTable,
                database.tables().get("rev:hasReview").toComplex(),
                "wsdbm:likes",
                JoinOn.OBJECT,
                "rev:hasReview",
                JoinOn.SUBJECT);
        LOG.info("Sort-Merge joined table size: {}", joinedTable.getValues().size());
        return joinedTable;
    }
}
