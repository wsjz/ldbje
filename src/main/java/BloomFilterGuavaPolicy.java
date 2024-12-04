import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.util.List;

public class BloomFilterGuavaPolicy implements FilterPolicy<BloomFilter<byte[]>> {

    private final int bitsPerKey;

    public BloomFilterGuavaPolicy(int bitsPerKey) {
        this.bitsPerKey = bitsPerKey;
    }

    @Override
    public String name() {
        return "leveldb.BuiltinBloomFilter2";
    }

    @Override
    public BloomFilter<byte[]> createFilter(List<byte[]> keys) {
        BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), bitsPerKey * keys.size(), 0.03);
        for (byte[] key : keys) {
            bloomFilter.put(key);
        }
        return bloomFilter;
    }

    @Override
    public boolean keyMayMatch(byte[] key, BloomFilter<byte[]> filter) {
        return filter.mightContain(key);
    }
}
