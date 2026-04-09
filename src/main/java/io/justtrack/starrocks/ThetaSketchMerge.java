package io.justtrack.starrocks;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;

import java.nio.ByteBuffer;
import java.util.Base64;

public class ThetaSketchMerge {
    private static final int NOMINAL_ENTRIES = 16384;
    private static final int SERIALIZED_STATE_BYTES = Integer.BYTES + Sketch.getMaxCompactSketchBytes(NOMINAL_ENTRIES);

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
        byte[] serialized = state.union.getResult().toByteArray();

        buff.putInt(serialized.length);
        buff.put(serialized);

        int padding = SERIALIZED_STATE_BYTES - Integer.BYTES - serialized.length;
        for (int i = 0; i < padding; i++) {
            buff.put((byte) 0);
        }
    }

    public void merge(State state, ByteBuffer buffer) {
        int serializedLength = buffer.getInt();
        byte[] serialized = new byte[serializedLength];
        buffer.get(serialized);

        state.union.union(Memory.wrap(serialized));

        int remainingPadding = SERIALIZED_STATE_BYTES - Integer.BYTES - serializedLength;
        buffer.position(buffer.position() + remainingPadding);
    }

    public String finalize(State state) {
        byte[] result = state.union.getResult().toByteArray();

        return Base64.getEncoder().encodeToString(result);
    }
}
