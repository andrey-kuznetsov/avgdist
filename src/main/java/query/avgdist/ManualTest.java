package query.avgdist;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class ManualTest {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Argument needed: directory with CSVs");
            return;
        }

        String csvDir = args[0];

        try (AverageDistances avgDist = new AverageDistancesImpl()) {
            System.out.println("Loading CSVs...");
            long loadStartNanos = System.nanoTime();
            avgDist.init(Paths.get(csvDir));
            long loadStopNanos = System.nanoTime();
            System.out.printf("CSVs loading took %10.5f s%n", 1.0 * (loadStopNanos - loadStartNanos) / 1e9);

            while (true) {
                LocalDateTime min = LocalDateTime.parse("2020-01-01 00:00:00", FORMATTER);
                LocalDateTime max = LocalDateTime.parse("2020-12-31 23:59:59", FORMATTER);

                long minTs = InputTuple.toUnixTimestamp(min);
                long maxTs = InputTuple.toUnixTimestamp(max);

                LocalDateTime left = min.plusSeconds(ThreadLocalRandom.current().nextLong(maxTs - minTs));
                LocalDateTime right = left.plusSeconds(ThreadLocalRandom.current().nextLong(maxTs - InputTuple.toUnixTimestamp(left)));

                query(avgDist, left, right);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void query(AverageDistances service, LocalDateTime from, LocalDateTime to) {
        System.out.println("Query: " + from + " - " + to);

        long startNanos = System.nanoTime();
        Map<Integer, Double> result = service.getAverageDistances(from, to);
        long stopNanos = System.nanoTime();

        System.out.println("Result size: " + result.size());
        result.forEach((key, value) -> System.out.println(key + " => " + value));

        System.out.printf("Execution time: %10.5f s%n", 1.0 * (stopNanos - startNanos) / 1e9);
    }
}
