import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;

public class Utils {
    public static long getUnsignedInt(long data) {
        return data & 0xFFFFFFFFL;
    }

    public static long hash(byte[] key) {
        return Utils.getUnsignedInt(Hashing.murmur3_32_fixed(0xbc9f1d34).hashBytes(key).hashCode());
    }

    public static long hash1(byte[] keys) {
        long seed = Utils.getUnsignedInt(0xbc9f1d34L);
        long m = Utils.getUnsignedInt(0xc6a4a793L);
        int limit = keys.length;
        long h = Utils.getUnsignedInt(seed ^ (keys.length * m));
        int tail = 0;
        ByteBuffer byteBuffer = ByteBuffer.wrap(keys);
        while (tail + 4 < keys.length) {
            long w = Utils.getUnsignedInt(byteBuffer.getInt());
            h = Utils.getUnsignedInt(h + w);
            h = Utils.getUnsignedInt(h * m);
            h = Utils.getUnsignedInt(h ^ (h >>> 16));
            tail += 4;
        }
        switch (limit - tail) {
            case 3:
                h += Utils.getUnsignedInt(keys[2] << 16);
                // fall through;;
            case 2:
                h += Utils.getUnsignedInt(keys[1] << 8);
                // fall through;
            case 1:
                h = Utils.getUnsignedInt(h + keys[0]);
                h = Utils.getUnsignedInt(h * m);
                h = Utils.getUnsignedInt(h ^ (h >>> 24));
                break;
        }
        return h;
    }

}
