package io.justtrack.starrocks;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ThetaSketchMergeV5 {
    private static final int NOMINAL_ENTRIES = 4096;
    private static final int LENGTH_PREFIX_BYTES = 5;
    private static final int SERIALIZED_STATE_BYTES = LENGTH_PREFIX_BYTES + maxBase64Length(Sketch.getMaxCompactSketchBytes(NOMINAL_ENTRIES));

    public static class State {
        Union union = SetOperation.builder().setNominalEntries(NOMINAL_ENTRIES).buildUnion();

        public int serializeLength() {
            return SERIALIZED_STATE_BYTES;
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public final void update(State state, String val) {
        byte[] decoded = Base64.getDecoder().decode(val);
        Sketch sketch = Sketches.wrapSketch(Memory.wrap(decoded));

        state.union.union(sketch);
    }

    public void serialize(State state, ByteBuffer buff) {
        byte[] serialized = Base64.getEncoder().encode(state.union.getResult().toByteArray());

        buff.put(lengthPrefix(serialized.length));
        buff.put(serialized);

        int padding = SERIALIZED_STATE_BYTES - LENGTH_PREFIX_BYTES - serialized.length;
        for (int i = 0; i < padding; i++) {
            buff.put((byte) ' ');
        }
    }

    public void merge(State state, ByteBuffer buffer) {
        byte[] lengthBytes = new byte[LENGTH_PREFIX_BYTES];
        buffer.get(lengthBytes);
        int serializedLength = Integer.parseInt(new String(lengthBytes, StandardCharsets.US_ASCII));
        byte[] serialized = new byte[serializedLength];
        buffer.get(serialized);

        state.union.union(Memory.wrap(Base64.getDecoder().decode(serialized)));

        int remainingPadding = SERIALIZED_STATE_BYTES - LENGTH_PREFIX_BYTES - serializedLength;
        buffer.position(buffer.position() + remainingPadding);
    }

    public String finalize(State state) {
        byte[] result = state.union.getResult().toByteArray();

        return Base64.getEncoder().encodeToString(result);
    }

    private static byte[] lengthPrefix(int length) {
        return String.format("%05d", length).getBytes(StandardCharsets.US_ASCII);
    }

    private static int maxBase64Length(int bytes) {
        return ((bytes + 2) / 3) * 4;
    }
}
