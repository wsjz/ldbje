import com.google.common.hash.BloomFilter;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;

class BloomFilterPolicyTest {

    private static BloomFilterPolicy policy;
    private static BloomFilterGuavaPolicy guavaPolicy;

    @Test
    void testEmptyFilter() {
        ByteBuffer filter = buildFilter(new ArrayList<>());
        filter.limit(0);
        Assertions.assertFalse(() -> matches(filter, "hello"));
        Assertions.assertFalse(() -> matches(filter, "world"));
    }

    @Test
    void testSimpleFilter() {
        List<String> keys = new ArrayList<>() {{
            add("hello");
            add("world");
        }};
        ByteBuffer filter = buildFilter(keys);
        Assertions.assertTrue(() -> matches(filter, "hello"));
        Assertions.assertTrue(() -> matches(filter, "world"));
        Assertions.assertFalse(() -> matches(filter, "x"));
        Assertions.assertFalse(() -> matches(filter, "foo"));
    }

    @Test
    void testFalsePositiveRate() {
        testRangeFalsePositiveRate(1, 80, 0.03, 0.02); // false positive rate < 2%
        System.err.println("-------------------------------------------------------");
        testRangeFalsePositiveRate(80, 100000, 0.0125, 0.01); // false positive rate < 1%
    }

    private static ByteBuffer buildFilter(List<String> keys) {
        policy = new BloomFilterPolicy(10);
        return policy.createFilter(keys.stream().map(String::getBytes).collect(Collectors.toList()));
    }

    private static boolean matches(ByteBuffer filter, String key) {
        return policy.keyMayMatch(key.getBytes(), filter);
    }

    static void testRangeFalsePositiveRate(int start, int end, double expectedRate, double goodRate) {
        // Count number of filters that significantly exceed the false positive rate
        int mediocreFilters = 0;
        int goodFilters = 0;
        for (long length = start; length <= end; length = nextLength(length)) {
            List<String> keys = new ArrayList<>();
            for (long i = 0; i < length; i++) {
                keys.add(String.valueOf(i));
            }
            ByteBuffer filter = buildFilter(keys);
            // System.out.printf("Max length: %s%n", (length * 10 / 8) + 40);
            Assertions.assertTrue(filter.capacity() < (length * 10 / 8) + 40);

            // All added keys must match
            for (long i = 0; i < length; i++) {
                String key = String.valueOf(i);
                // System.out.printf("Length: %d, Key: %s%n", length, key);
                Assertions.assertTrue(matches(filter, key));
            }

            // Check false positive rate
            double rate = falsePositiveRate(filter);
            System.err.printf("False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                    rate * 100.0, length, filter.capacity());
            Assertions.assertTrue(rate < expectedRate);  // Must not be over 2%
            if (rate > goodRate) {
                mediocreFilters++;  // Allowed, but not too often
            } else {
                goodFilters++;
            }
        }
        System.err.printf("Filters: %d good, %d mediocre\n", goodFilters, mediocreFilters);
        Assertions.assertTrue(mediocreFilters < (goodFilters / 5));
    }

    private static double falsePositiveRate(ByteBuffer filter) {
        long result = 0;
        for (long i = 0; i < 10000; i++) {
            if (matches(filter, String.valueOf(i + 1000000000))) {
                result++;
            }
        }
        return result / 10000.0;
    }

    private static long nextLength(long length) {
        if (length < 10) {
            length += 1;
        } else if (length < 100) {
            length += 10;
        } else if (length < 1000) {
            length += 100;
        } else if (length < 10000) {
            length += 1000;
        } else {
            length += 10000;
        }
        return length;
    }

    @Test
    void testSimpleFilterG() {
        List<String> keys = new ArrayList<>() {{
            add("hello");
            add("world");
        }};
        BloomFilter<byte[]> filter = buildGuavaFilter(keys);
        Assertions.assertTrue(() -> matchesG(filter, "hello"));
        Assertions.assertTrue(() -> matchesG(filter, "world"));
        Assertions.assertFalse(() -> matchesG(filter, "xxxxx"));
        Assertions.assertFalse(() -> matchesG(filter, "foo"));
    }

    @Test
    void testGuavaFalsePositiveRate() {
        testGuavaRangeFalsePositiveRate(1, 80, 0.02); // false positive rate < 2%
        System.err.println("-------------------------------------------------------");
        testGuavaRangeFalsePositiveRate(80, 100000, 0.01); // false positive rate < 1%
    }

    static void testGuavaRangeFalsePositiveRate(int start, int end, double goodRate) {
        // Count number of filters that significantly exceed the false positive rate
        int mediocreFilters = 0;
        int goodFilters = 0;
        for (long length = start; length <= end; length = nextLength(length)) {
            List<String> keys = new ArrayList<>();
            for (long i = 0; i < length; i++) {
                keys.add(String.valueOf(i));
            }
            BloomFilter<byte[]> filter = buildGuavaFilter(keys);
            // System.out.printf("Max length: %s%n", (length * 10 / 8) + 40);
            Assertions.assertTrue(filter.approximateElementCount() < (length * 10 / 8) + 40);

            // All added keys must match
            for (long i = 0; i < length; i++) {
                String key = String.valueOf(i);
                // System.out.printf("Length: %d, Key: %s%n", length, key);
                Assertions.assertTrue(matchesG(filter, key));
            }

            // Check false positive rate
            double rate = falsePositiveRateG(filter);
            System.err.printf("False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                    rate * 100.0, length, filter.approximateElementCount());
            if (rate > goodRate) {
                mediocreFilters++;  // Allowed, but not too often
            } else {
                goodFilters++;
            }
        }
        System.err.printf("Filters: %d good, %d mediocre\n", goodFilters, mediocreFilters);
        Assertions.assertTrue(mediocreFilters < (goodFilters / 5));
    }

    private static BloomFilter<byte[]> buildGuavaFilter(List<String> keys) {
        guavaPolicy = new BloomFilterGuavaPolicy(10);
        return guavaPolicy.createFilter(keys.stream().map(String::getBytes).collect(Collectors.toList()));
    }

    private static double falsePositiveRateG(BloomFilter<byte[]> filter) {
        long result = 0;
        for (long i = 0; i < 10000; i++) {
            if (matchesG(filter, String.valueOf(i + 1000000000))) {
                result++;
            }
        }
        return result / 10000.0;
    }

    private static boolean matchesG(BloomFilter<byte[]> filter, String key) {
        return guavaPolicy.keyMayMatch(key.getBytes(), filter);
    }
}