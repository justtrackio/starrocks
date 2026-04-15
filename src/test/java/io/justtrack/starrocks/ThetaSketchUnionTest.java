package io.justtrack.starrocks;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThetaSketchUnionTest {
    private final ThetaSketchUnion aggregator = new ThetaSketchUnion();

    @Test
    void finalizeOnFreshStateReturnsEmptySketch() {
        ThetaSketchUnion.State state = aggregator.create();

        assertTrue(state.serializeLength() > 0);

        Sketch result = Sketches.wrapSketch(Memory.wrap(aggregator.finalize(state)));

        assertEquals(0.0, result.getEstimate());
    }

    @Test
    void updateMergesDistinctItemsAcrossSketches() {
        ThetaSketchUnion.State state = aggregator.create();

        aggregator.update(state, fromBase64("AQMDAAA6zJNsfIqYmZiAaQ=="));
        aggregator.update(state, fromBase64("AQMDAAA6zJMj3IAxxCv4RA=="));

        Sketch result = Sketches.wrapSketch(Memory.wrap(aggregator.finalize(state)));

        assertEquals(2.0, result.getEstimate());
    }

    @Test
    void serializeAndMergeRoundTripPreservesUnion() {
        ThetaSketchUnion.State source = aggregator.create();
        aggregator.update(source, fromBase64("AQMDAAA6zJNsfIqYmZiAaQ=="));
        aggregator.update(source, fromBase64("AQMDAAA6zJMj3IAxxCv4RA=="));

        ByteBuffer buffer = ByteBuffer.allocate(source.serializeLength());
        aggregator.serialize(source, buffer);
        buffer.flip();

        ThetaSketchUnion.State target = aggregator.create();
        aggregator.merge(target, buffer);

        Sketch result = Sketches.wrapSketch(Memory.wrap(aggregator.finalize(target)));

        assertEquals(2.0, result.getEstimate());
    }

    @Test
    void serializeLengthMatchesWrittenStateSize() {
        ThetaSketchUnion.State state = aggregator.create();
        aggregator.update(state, encodedSketch("one", "two", "three"));

        ByteBuffer buffer = ByteBuffer.allocate(state.serializeLength());
        aggregator.serialize(state, buffer);

        assertEquals(state.serializeLength(), buffer.position());
    }

    @Test
    void serializeLengthFitsWithinStarRocksVarcharLimit() {
        ThetaSketchUnion.State state = aggregator.create();

        assertTrue(state.serializeLength() < 65533);
    }

    @Test
    void mergeCombinesExistingAndSerializedState() {
        ThetaSketchUnion.State left = aggregator.create();
        ThetaSketchUnion.State right = aggregator.create();

        aggregator.update(left, encodedSketch("a", "b"));
        aggregator.update(right, encodedSketch("b", "c", "d"));

        ByteBuffer buffer = ByteBuffer.allocate(right.serializeLength());
        aggregator.serialize(right, buffer);
        buffer.flip();

        aggregator.merge(left, buffer);

        Sketch result = Sketches.wrapSketch(Memory.wrap(aggregator.finalize(left)));

        assertEquals(4.0, result.getEstimate());
    }

    private static byte[] encodedSketch(String... values) {
        UpdateSketch sketch = UpdateSketch.builder().build();
        for (String value : values) {
            sketch.update(value);
        }

        return sketch.compact().toByteArray();
    }

    private static Sketch decodeSketch(String encodedSketch) {
        byte[] bytes = Base64.getDecoder().decode(encodedSketch);
        return Sketches.wrapSketch(Memory.wrap(bytes));
    }

    private static byte[] fromBase64(String encodedSketch) {
        return Base64.getDecoder().decode(encodedSketch);
    }
}
