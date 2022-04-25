package query.avgdist;

import com.brein.time.timeintervals.collections.ListIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.indexes.IntervalValueComparator;
import com.brein.time.timeintervals.intervals.LongInterval;
import de.siegmar.fastcsv.reader.CsvReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Interval tree-based implementation of {@link AverageDistances}.
 */
public class AverageDistancesImpl implements AverageDistances {
    /**
     * Debug output switch.
     */
    private final static boolean DEBUG_ENABLED = System.getProperty("debug") != null;

    /**
     * Size of thread pool for parallel query processing.
     */
    private final int executorThreadCount;

    /**
     * Maximum allowed query execution time.
     */
    private final long queryTimeoutSeconds;

    /**
     * The executor for parallel query processing.
     */
    private ThreadPoolExecutor executor;

    /**
     * Separate interval tree for each passengerCount value.
     */
    private final Map<Integer, IntervalTree> treePerPassCnt = new HashMap<>();

    /**
     * Common spread of all interval trees.
     */
    LongInterval spread;

    /**
     * Creates an instance with default settings.
     */
    public AverageDistancesImpl() {
        this(Math.max(Runtime.getRuntime().availableProcessors() * 4 / 5, 2), Long.MAX_VALUE);
    }

    /**
     * Creates an instance.
     *
     * @param executorThreadCount size of thread pool for parallel query processing.
     * @param queryTimeoutSeconds maximum allowed query execution time.
     */
    public AverageDistancesImpl(int executorThreadCount, long queryTimeoutSeconds) {
        this.executorThreadCount = executorThreadCount;

        if (queryTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("Query timeout value should be positive");
        }

        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    @Override
    public void init(Path dataDir) {
        close();

        File dir = dataDir.toFile();
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException(dir + " is not a directory");
        }

        File[] csvFiles = dir.listFiles((d, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            throw new IllegalArgumentException("Failed to find CSV files in " + dir);
        }

        MutableLong minTimestamp = new MutableLong(Long.MAX_VALUE);
        MutableLong maxTimestamp = new MutableLong(Long.MIN_VALUE);

        for (File csvFile : csvFiles) {
            try (CsvReader csvReader = CsvReader.builder()
                .skipEmptyRows(true)
                .errorOnDifferentFieldCount(false)
                .build(csvFile.toPath().toAbsolutePath())) {

                String fileName = csvFile.getName();
                debug("Loading " + fileName);

                csvReader.stream()
                    .skip(1)
                    .forEach(row -> InputTuple.parseCsvRow(row, fileName, AverageDistancesImpl::warn)
                        .ifPresent(tuple -> {
                            if (row.getOriginalLineNumber() % 200000 == 0) {
                                debug(row.getOriginalLineNumber() + " rows loaded");
                            }
                            if (tuple.pickupTimestamp <= tuple.dropoffTimestamp) {
                                treePerPassCnt.computeIfAbsent(tuple.passengerCount, passCnt -> createTimestampIntervalTree())
                                    .add(new IntervalWithDistance(tuple.pickupTimestamp, tuple.dropoffTimestamp, tuple.distance));
                                if (tuple.pickupTimestamp < minTimestamp.value) {
                                    minTimestamp.value = tuple.pickupTimestamp;
                                }
                                if (tuple.dropoffTimestamp > maxTimestamp.value) {
                                    maxTimestamp.value = tuple.dropoffTimestamp;
                                }
                            } else {
                                warn(String.format("Dropping inverted time interval found in %s@%d",
                                    fileName, row.getOriginalLineNumber()));
                            }
                        }));
            } catch (IOException e) {
                letItCrash("Failed to read " + csvFile + ", message: " + e.getMessage());
            }
        }

        if (!treePerPassCnt.isEmpty()) {
            shouldBeImpossible(executor != null, "executor should be null after close() call");

            executor = new ThreadPoolExecutor(executorThreadCount, executorThreadCount,
                Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

            spread = new LongInterval(minTimestamp.value, maxTimestamp.value);
            debug("timestamp spread: " + spread);
        }
    }

    @Override
    public Map<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
        if (treePerPassCnt.isEmpty()) {
            return Collections.emptyMap();
        }

        LongInterval initialQuery = new LongInterval(InputTuple.toUnixTimestamp(start), InputTuple.toUnixTimestamp(end));
        Optional<LongInterval> query = intersection(initialQuery, spread);

        if (!query.isPresent()) {
            return Collections.emptyMap();
        }

        Map<Integer, DoubleAccumulator> accumulators = new HashMap<>();

        List<QueryJob> jobs = treePerPassCnt.entrySet().stream()
            .map(entry -> new QueryJob(
                entry.getValue(),
                query.get(),
                accumulators.computeIfAbsent(entry.getKey(), k -> new DoubleAccumulator())))
            .peek(job -> debug("initial job: " + job.queryInterval + " " + job.accumulator))
            .collect(Collectors.toList());

        List<CompletableFuture<?>> runningJobs = optimizedJobCollection(jobs).stream()
            .peek(job -> debug("optimized job: " + job.queryInterval + " " + job.accumulator))
            .peek(job -> shouldBeImpossible(executor == null, "no executor exists"))
            .map(job -> CompletableFuture.runAsync(job, executor))
            .collect(Collectors.toList());

        try {
            CompletableFuture.allOf(runningJobs.toArray(new CompletableFuture[] {})).get(queryTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        return accumulators.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().count.longValue() != 0
                ? e.getValue().value.doubleValue() / e.getValue().count.longValue()
                : 0.0));
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
        treePerPassCnt.clear();
    }

    private static IntervalTree createTimestampIntervalTree() {
        return IntervalTreeBuilder.newBuilder()
            .collectIntervals(interval -> new ListIntervalCollection())
            .overrideComparator(IntervalValueComparator::compareLongs)
            .build();
    }

    /**
     * If job count is smaller than available thread count, then CPU resources are underutilized,
     * and jobs should be broken to lesser jobs by fragmenting query intervals.
     *
     * @param jobs input job list.
     * @return output job collection, can be the same is input if no optimization needed.
     */
    private Collection<QueryJob> optimizedJobCollection(List<QueryJob> jobs) {
        // algorithm parameters; subject to investigation/tuning
        final double minJobsPerThread = 1.5;
        final double minJobWidthMillis = TimeUnit.DAYS.toMillis(3);

        int desiredJobCount = (int) (executorThreadCount * minJobsPerThread);

        if (jobs.isEmpty() || jobs.size() >= desiredJobCount) {
            return jobs;
        }

        TreeMap<Long, List<QueryJob>> jobsByIntervalWidth = new TreeMap<>();
        jobs.forEach(job -> jobsByIntervalWidth.computeIfAbsent(width(job.queryInterval), k -> new ArrayList<>()).add(job));
        int jobCount = jobs.size();

        do {
            Long maxJobWidth = jobsByIntervalWidth.lastEntry().getKey();
            if (maxJobWidth < minJobWidthMillis) {
                break;
            }

            List<QueryJob> widestJobs = jobsByIntervalWidth.lastEntry().getValue();
            long newWidth = maxJobWidth / 2;
            List<QueryJob> lesserJobs = widestJobs.stream()
                .flatMap(job -> {
                    long left = job.queryInterval.getStart();
                    long center = job.queryInterval.getStart() + newWidth;
                    long right = job.queryInterval.getEnd();
                    return Stream.of(
                        new QueryJob(job.searchTree, new LongInterval(left, center), job.accumulator),
                        new QueryJob(job.searchTree, new LongInterval(center, right), job.accumulator)
                    );
                })
                .collect(Collectors.toList());

            jobsByIntervalWidth.remove(maxJobWidth);
            jobsByIntervalWidth.put(newWidth, lesserJobs);
            jobCount += widestJobs.size();
        } while (jobCount < desiredJobCount);

        return jobsByIntervalWidth.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
    }

    private static Optional<LongInterval> intersection(LongInterval i1, LongInterval i2) {
        if (i1.getEnd() < i2.getStart() || i2.getEnd() < i1.getStart()) {
            return Optional.empty();
        }

        long end = Math.min(i1.getEnd(), i2.getEnd());

        LongInterval result = i1.getStart() <= i2.getStart()
            ? new LongInterval(i2.getStart(), end)
            : new LongInterval(i1.getStart(), end);

        return Optional.of(result);
    }

    private static long width(LongInterval interval) {
        return interval.getEnd() - interval.getStart();
    }

    private static void warn(String msg) {
        System.err.println(msg);
    }

    private static void debug(String msg) {
        if (DEBUG_ENABLED) {
            System.out.println(msg);
        }
    }

    private static void shouldBeImpossible(boolean condition, String message) {
        if (condition) {
            letItCrash("Internal error: " + message);
        }
    }

    private static void letItCrash(String message) {
        throw new RuntimeException(message);
    }
}
