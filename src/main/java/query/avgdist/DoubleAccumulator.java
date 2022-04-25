package query.avgdist;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Accumulates double values and their number from multiple threads.
 */
class DoubleAccumulator {
    public final LongAdder count = new LongAdder();
    public final DoubleAdder value = new DoubleAdder();

    public void add(double v) {
        count.increment();
        value.add(v);
    }
}
