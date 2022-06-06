package uni.freiburg.sparqljoin.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Class provides some performance functionalities
 */
public class Performance {

    private Performance(){}

    private static final Logger LOG = LoggerFactory.getLogger(Performance.class);

    /**
     *  Method could be used as a wrapper for any other method to measure the incoming method duration
     *
     * @param s          method that should be measured
     * @param methodName name of the method for logging purposes
     * @return           the actual result of the incoming method
     */
    public static <T> T measure(final Supplier<T> s, final String methodName) {
        LOG.debug("Starting measurement of '{}'...", methodName);
        Instant start = Instant.now();
        T result = s.get();
        Duration duration = Duration.between(start, Instant.now());
        String secondsWithMillis = String.format("%.3f", duration.toMillis() / 1000f);
        LOG.info("Stage '{}' took {} seconds", methodName, secondsWithMillis);
        return result;
    }
}
