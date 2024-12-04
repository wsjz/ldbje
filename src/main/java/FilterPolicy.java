import java.nio.ByteBuffer;
import java.util.List;

public interface FilterPolicy {

    String name();

    ByteBuffer createFilter(List<byte[]> keys);

    boolean keyMayMatch(byte[] key, ByteBuffer filter);
}
