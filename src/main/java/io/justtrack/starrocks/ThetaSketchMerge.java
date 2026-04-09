package io.justtrack.starrocks;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
        ByteBuffer serializedView = nativeOrderSlice(buff);
        WritableMemory memory = WritableMemory.writableWrap(serializedView);
        CompactSketch serialized = state.union.getResult(false, memory);

        buff.position(buff.position() + serialized.getCurrentBytes());
    }

    public void merge(State state, ByteBuffer buffer) {
        ByteBuffer serializedView = nativeOrderSlice(buffer);
        state.union.union(Memory.wrap(serializedView));
        buffer.position(buffer.limit());
    }

    public String finalize(State state) {
        byte[] result = state.union.getResult().toByteArray();

        return Base64.getEncoder().encodeToString(result);
    }

    private static ByteBuffer nativeOrderSlice(ByteBuffer buffer) {
        return buffer.slice().order(ByteOrder.nativeOrder());
    }
}
