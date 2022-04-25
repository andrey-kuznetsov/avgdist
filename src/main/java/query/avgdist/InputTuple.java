package query.avgdist;

import de.siegmar.fastcsv.reader.CsvRow;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Essential fields of CSV row.
 */
class InputTuple {
    private static final DateTimeFormatter CSV_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    long pickupTimestamp;
    long dropoffTimestamp;
    int passengerCount;
    double distance;

    /**
     * Do not construct. Use {@link #parseCsvRow(CsvRow, String, Consumer)}.
     */
    private InputTuple() {
    }

    /**
     * @return parsed row or empty {@code Optional} if failed.
     */
    public static Optional<InputTuple> parseCsvRow(CsvRow row, String fileName, Consumer<String> errorReporter) {
        InputTuple result = new InputTuple();

        try {
            result.pickupTimestamp = toUnixTimestamp(LocalDateTime.parse(row.getField(1), CSV_DATE_TIME_FORMAT));
            result.dropoffTimestamp = toUnixTimestamp(LocalDateTime.parse(row.getField(2), CSV_DATE_TIME_FORMAT));
        } catch (DateTimeParseException e) {
            errorReporter.accept(String.format("CSV datetime parsing error in %s@%d: %s",
                fileName, row.getOriginalLineNumber(), e.getMessage()));
            return Optional.empty();
        }

        try {
            result.passengerCount = (int) Double.parseDouble(row.getField(3));
            result.distance = Double.parseDouble(row.getField(4));
        } catch (NumberFormatException e) {
            errorReporter.accept(String.format("CSV number parsing error in %s@%d: %s",
                fileName, row.getOriginalLineNumber(), e.getMessage()));
            return Optional.empty();
        }

        return Optional.of(result);
    }

    public static long toUnixTimestamp(LocalDateTime dateTime) {
        return dateTime.toEpochSecond(ZoneOffset.UTC);
    }
}
