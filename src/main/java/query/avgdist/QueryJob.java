package query.avgdist;

import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.intervals.LongInterval;

/**
 * Job that searches in IntervalTree of {@link IntervalWithDistance}s.
 */
class QueryJob implements Runnable {
    public final IntervalTree searchTree;

    public final LongInterval queryInterval;

    public final DoubleAccumulator accumulator;

    public QueryJob(IntervalTree searchTree, LongInterval queryInterval, DoubleAccumulator accumulator) {
        this.searchTree = searchTree;
        this.queryInterval = queryInterval;
        this.accumulator = accumulator;
    }

    @Override
    public void run() {
        // Interval tree structure can't find intervals inside query interval,
        // it looks for all overlapping intervals, so additional filtering is required.
        // IntervalTree implementation can return overlap() result in a form of stream,
        // so the filtering does not change time complexity of log n + m.
        // (Filtering contributes only to a coefficient of m).
        searchTree.overlapStream(queryInterval)
            .map(IntervalWithDistance.class::cast)
            .filter(interval -> contains(queryInterval, interval))
            .forEach(interval -> accumulator.add(interval.distance));
    }

    private static boolean contains(LongInterval container, LongInterval candidate) {
        return container.getStart() <= candidate.getStart() && candidate.getEnd() <= container.getEnd();
    }
}
