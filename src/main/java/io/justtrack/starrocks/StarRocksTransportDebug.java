package io.justtrack.starrocks;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class StarRocksTransportDebug {
    private static final int SERIALIZED_STATE_BYTES = 256;
    private static final int LENGTH_PREFIX_BYTES = 3;
    private static final int CAPTURE_BYTES = 64;

    public static class State {
        String value = "";

        public int serializeLength() {
            return SERIALIZED_STATE_BYTES;
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void update(State state, String val) {
        if (state.value.isEmpty() && val != null) {
            state.value = val;
        }
    }

    public void serialize(State state, ByteBuffer buff) {
        byte[] payload = state.value.getBytes(StandardCharsets.US_ASCII);
        byte[] length = String.format("%03d", payload.length).getBytes(StandardCharsets.US_ASCII);

        buff.put(length);
        buff.put(payload);

        int padding = SERIALIZED_STATE_BYTES - LENGTH_PREFIX_BYTES - payload.length;
        for (int i = 0; i < padding; i++) {
            buff.put((byte) '.');
        }
    }

    public void merge(State state, ByteBuffer buffer) {
        byte[] captured = new byte[CAPTURE_BYTES];
        buffer.get(captured);
        state.value = Base64.getEncoder().encodeToString(captured);
        buffer.position(buffer.limit());
    }

    public String finalize(State state) {
        return state.value;
    }
}
