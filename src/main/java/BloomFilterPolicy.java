import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.util.List;

public class BloomFilterPolicy implements FilterPolicy<ByteBuffer> {

    private final int bitsPerKey;
    private int k;

    public BloomFilterPolicy(int bitsPerKey) {
        this.bitsPerKey = bitsPerKey;
        // 哈希函数的个数k，位数组的长度 m，数据集大小n;
        // 1. 为了获得最优的准确率，当k = ln2 * (m/n)时，布隆过滤器获得最优的准确性；
        // 2. 在哈希函数的个数取到最优时，要让错误率不超过є，m至少需要取到最小值的1.44倍；
        // 创建一个布隆过滤器时，只需要指定为每个key分配的位数，该值（m/n）大于1.44即可
        k = (int) (bitsPerKey * 0.69);  // 0.69 =~ ln(2)
        if (k < 1) {
            k = 1;
        }
        if (k > 30) {
            k = 30;
        }
    }

    @Override
    public String name() {
        return "leveldb.BuiltinBloomFilter";
    }

    @Override
    public ByteBuffer createFilter(List<byte[]> keys) {
        int n = keys.size();
        int bits = n * bitsPerKey;
        // For small n, we can see a very high false positive rate.
        // Fix it by enforcing a minimum bloom filter length.
        if (bits < 64) {
            bits = 64;
        }
        int bytes = (bits + 7) / 8;
        bits = bytes * 8;

        // put hash num k into tail
        ByteBuffer result = ByteBuffer.allocate(bytes + 1)
                .position(bytes)
                .put((byte) k)
                .position(0);
        for (byte[] key : keys) {
            long h = bloomHash(key);
            // double hashing开放定址法
            long delta = reHashing(h);
            for (int j = 0; j < k; j++) {
                int bitPos = (int) (h % bits);
                byte bitVal = result.position(bitPos / 8).get();
                bitVal |= (byte) (1 << (bitPos % 8));
                result.position(bitPos / 8).put(bitVal);
                h += delta;
            }
        }
        return result;
    }

    @Override
    public boolean keyMayMatch(byte[] key, ByteBuffer filter) {
        int len = filter.limit();
        if (len < 2) {
            return false;
        }
        int bits = (len - 1) * 8;
        byte k = filter.get(len - 1);
        if (k > 30) {
            return true;
        }
        long h = bloomHash(key);
        long delta = reHashing(h);
        for (int j = 0; j < k; j++) {
            int bitPos = (int) (h % bits);
            if ((filter.get(bitPos / 8) & (1 << (bitPos % 8))) == 0) {
                return false;
            }
            h += delta;
        }
        return true;
    }

    private static long bloomHash(byte[] key) {
        return Utils.hash(key);
    }

    private static long reHashing(long h) {
        return Utils.getUnsignedInt(Hashing.murmur3_128().hashLong(h).hashCode());
    }
}
