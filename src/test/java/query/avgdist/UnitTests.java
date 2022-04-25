package query.avgdist;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnitTests {
    private static final LocalDateTime MIN_DATETIME = toDateTime("2020-01-01 00:00:00");
    private static final LocalDateTime MAX_DATETIME = toDateTime("2020-12-31 23:59:59");

    private static final double DELTA = 1e-6;

    @Test
    public void emptyDir() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("src"));
            assertTrue(impl.getAverageDistances(MIN_DATETIME, MAX_DATETIME).isEmpty());
        }
    }

    @Test
    public void reinit() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("testdata", "simple"));
            assertEquals(1, impl.getAverageDistances(MIN_DATETIME, MAX_DATETIME).size());
            impl.init(Paths.get("testdata", "12days"));
            assertEquals(12, impl.getAverageDistances(MIN_DATETIME, MAX_DATETIME).size());
            impl.init(Paths.get("src"));
            assertTrue(impl.getAverageDistances(MIN_DATETIME, MAX_DATETIME).isEmpty());
        }
    }

    @Test
    public void severalFiles() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("testdata", "severalFiles"));

            Map<Integer, Double> result = impl.getAverageDistances(MIN_DATETIME, MAX_DATETIME);
            assertEquals(1, result.size());
            assertEquals(10.0, result.get(1));
        }
    }

    @Test
    public void simple12DaysQueries() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("testdata", "12days"));

            for (int month = 1; month <= 12; month++) {
                Map<Integer, Double> result = impl.getAverageDistances(LocalDateTime.of(2020, month, 1, 0, 0, 0), LocalDateTime.of(2020, month, 2, 0, 0, 0));
                assertEquals(month, result.get(month));
            }
        }
    }

    @Test
    public void avgCalculationSanity() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("testdata", "2passCntValues"));

            Map<Integer, Double> result = impl.getAverageDistances(toDateTime("2020-01-01 00:00:00"), toDateTime("2020-01-02 00:00:00"));
            assertEquals(2, result.size());
            assertEquals(2.0, result.get(1), DELTA);
            assertEquals(10.5, result.get(2), DELTA);

            result = impl.getAverageDistances(toDateTime("2020-01-01 00:00:00"), toDateTime("2020-01-01 14:00:00"));
            assertEquals(1.5, result.get(1), DELTA);
            assertEquals(7.5, result.get(2), DELTA);
       }
    }

    @Test
    public void invertedQuery() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("testdata", "simple"));
            assertThrows(Exception.class, () -> impl.getAverageDistances(toDateTime("2020-12-01 00:00:00"), toDateTime("2020-01-01 00:00:00")));
        }
    }

    @Test
    public void invertedDataInterval() {
        try (AverageDistancesImpl impl = new AverageDistancesImpl()) {
            impl.init(Paths.get("testdata", "invertedInterval"));

            Map<Integer, Double> result = impl.getAverageDistances(MIN_DATETIME, MAX_DATETIME);
            assertTrue(result.isEmpty());
        }
    }

    private static LocalDateTime toDateTime(String from) {
        return LocalDateTime.parse(from, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
