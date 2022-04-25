package query.avgdist;

import com.brein.time.exceptions.IllegalTimeInterval;
import com.brein.time.exceptions.IllegalTimePoint;
import com.brein.time.timeintervals.intervals.LongInterval;

/**
 * Interval datatype to be stored in IntervalTree with distance value attached.
 */
class IntervalWithDistance extends LongInterval {
    public final double distance;

    public IntervalWithDistance() {
        super();
        distance = 0.0;
    }

    public IntervalWithDistance(Long start, Long end, double distance)
        throws IllegalTimeInterval, IllegalTimePoint {
        super(start, end);
        this.distance = distance;
    }
}
