package io.justtrack.starrocks;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;

import java.nio.ByteBuffer;
import java.util.Base64;

public class ThetaSketchUnion {
    private static final int NOMINAL_ENTRIES = 4096;

    public static class State {
        Union union = SetOperation.builder().setNominalEntries(NOMINAL_ENTRIES).buildUnion();

        public int serializeLength() {
            return 4 + union.getResult().getCompactBytes();
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
    }

    public void merge(State state, ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);

        Sketch sketch = Sketches.wrapSketch(Memory.wrap(bytes));
        state.union.union(sketch);
    }

    public String finalize(State state) {
        byte[] result = state.union.getResult().toByteArray();

        return Base64.getEncoder().encodeToString(result);
    }
}
