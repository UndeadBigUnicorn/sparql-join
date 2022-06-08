package uni.freiburg.sparqljoin.util;

/**
 * Class provides hash methods
 */
public class Hasher {

    public static <T> long hash(T value) {
        if (value instanceof Integer) {
            return hash(((Integer) value).longValue());
        } else if (value instanceof Long) {
            return hash((Long) value);
        } else if (value instanceof String) {
            return hash((String) value);
        } else {
            return hash(String.valueOf(value));
        }
    }

    /**
     * Hash string by SDBDM algorithm
     * @param s to hash
     * @return hashed value
     */
    public static long hash(final String s) {
        long hash = 0;
        for(int i = 0; i < s.length(); ++i) {
            hash = s.charAt(i) + (hash << 6) + (hash << 16) - hash;
        }
        return hash;
    }

    /**
     * Hash int
     * @param a to hash
     * @return hashed value
     */
    public static long hash(long a) {
        a ^= (a << 13);
        a ^= (a >>> 17);
        a ^= (a << 5);
        return a;
    }
}
