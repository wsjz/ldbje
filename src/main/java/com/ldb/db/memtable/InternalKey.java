package com.ldb.db.memtable;

import sun.jvm.hotspot.ci.ciMethodData;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public class InternalKey {
    private ByteBuffer rep;

    enum ValueType {kTypeDeletion, kTypeValue,}

    InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
    InternalKey(String userKey, long sequenceNumber, ValueType t) {
        appendInternalKey(rep, new ParsedInternalKey(userKey, sequenceNumber, t));
    }

    public boolean decodeFrom(ByteBuffer s) {
        rep = s;
        return !rep.hasRemaining();
    }

    public ByteBuffer encode() {
        assert(!rep.hasRemaining());
        return rep;
    }

    public String userKey() {
        return extractUserKey(rep);
    }

    private String extractUserKey(ByteBuffer rep) {

    }

    public void setFrom(ParsedInternalKey p) {
        rep.clear();
        appendInternalKey(rep, p);
    }

    private void appendInternalKey(ByteBuffer result, ParsedInternalKey key) {
        result.put(result);
        // putFixed64(result, packSequenceAndType(key.sequence, key.type));
    }

    public void clear() { rep.clear(); }

    public String debugString() {
        ParsedInternalKey parsed = new ParsedInternalKey();
        if (ParseInternalKey(rep, &parsed)) {
            return parsed.debugString();
        }
        StringBuilder ss = new StringBuilder();
        ss.append("(bad)").append(escapeString(rep));
        return ss.toString();
    }

    private static class ParsedInternalKey {

        public ByteBuffer userKey;
        long sequence;
        ValueType type;
        public String debugString() {
            StringBuilder ss = new StringBuilder();
            ss.append('\\').append(escapeString(userKey)).append("' @ ")
                    .append(sequence)
                    .append(" : ")
                    .append(type);
            return ss.toString();
        }
    }

    private static char[] escapeString(ByteBuffer rep) {
        return rep.asCharBuffer().array();
    }
}
