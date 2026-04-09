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

    public static class State {
        Union union = SetOperation.builder().setNominalEntries(NOMINAL_ENTRIES).buildUnion();

        public int serializeLength() {
            return Sketch.getMaxCompactSketchBytes(NOMINAL_ENTRIES);
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
        // DataSketches 6.2.0 uses JDK-internal direct ByteBuffer access that is blocked on Java 17.
        // Serializing through a byte array keeps heap and direct buffers working across JDKs.
        buff.put(state.union.getResult().toByteArray());
    }

    public void merge(State state, ByteBuffer buffer) {
        ByteBuffer serializedView = buffer.slice();
        byte[] serialized = new byte[serializedView.remaining()];
        serializedView.get(serialized);

        state.union.union(Memory.wrap(serialized));
        buffer.position(buffer.limit());
    }

    public String finalize(State state) {
        byte[] result = state.union.getResult().toByteArray();

        return Base64.getEncoder().encodeToString(result);
    }
}
