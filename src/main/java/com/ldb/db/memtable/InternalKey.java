package com.ldb.db.memtable;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

@Setter
@Getter
public class InternalKey {
    private ByteBuffer rep;
    enum ValueType {kTypeDeletion, kTypeValue,}

    InternalKey() {}  // Leave rep as empty to indicate it is invalid
    InternalKey(ByteBuffer userKey, long sequenceNumber, ValueType t) {
        rep = appendInternalKey(new ParsedInternalKey(userKey, sequenceNumber, t));
    }

    public ByteBuffer userKey() {
        return extractUserKey(rep);
    }

    public void clear() { rep.clear(); }

    public String debugString() {
        try {
            return ParsedInternalKey.fromBytes(rep).debugString();
        } catch (Exception e) {
            return "(bad)" + rep;
        }
    }

    private static ByteBuffer extractUserKey(ByteBuffer rep) {
        byte[] bytes = new byte[rep.capacity() - getSuffix()];
        rep.get(bytes, 0, bytes.length);
        return ByteBuffer.wrap(bytes);
    }

    private static ByteBuffer appendInternalKey(ParsedInternalKey key) {
        ByteBuffer result = ByteBuffer.allocate(key.userKey.capacity() + getSuffix());
        result.put(key.userKey);
        result.putLong(key.sequenceNumber);
        result.putInt(key.type.ordinal());
        return result;
    }

    private static int getSuffix() {
        return Long.BYTES + Integer.BYTES;
    }

    public static class ParsedInternalKey {
        public ByteBuffer userKey;
        long sequenceNumber;
        ValueType type;

        public ParsedInternalKey(ByteBuffer userKey, long sequenceNumber, ValueType type) {
            this.userKey = userKey;
            this.sequenceNumber = sequenceNumber;
            this.type = type;
        }

        public String debugString() {
            return '\\' + new String(userKey.array()) + "' @ " + sequenceNumber + " : " + type;
        }

        public static ParsedInternalKey fromBytes(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.capacity() - getSuffix()];
            buffer.get(bytes, 0, bytes.length);
            ByteBuffer userKey = ByteBuffer.wrap(bytes);
            long sequenceNumber = buffer.getLong();
            int type = buffer.getInt();
            if (type > ValueType.kTypeValue.ordinal()) {
                throw new IllegalArgumentException();
            } else {
                return new ParsedInternalKey(userKey, sequenceNumber, ValueType.values()[type]);
            }
        }
    }
}
