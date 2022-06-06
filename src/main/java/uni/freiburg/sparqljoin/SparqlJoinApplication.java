package uni.freiburg.sparqljoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uni.freiburg.sparqljoin.service.DataLoaderService;
import uni.freiburg.sparqljoin.util.Performance;

@SpringBootApplication
public class SparqlJoinApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparqlJoinApplication.class);

    @Autowired
    DataLoaderService dataLoaderService;

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
        Performance.measure(this::loadData, "Load Data");
        return true;
    }

    public boolean loadData() {
        dataLoaderService.load(datasetPath);
        return true;
    }
}
