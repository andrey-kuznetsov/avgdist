# Average Distance Querying Implementation

## Approach Choice: In-memory vs. On-disk/Mixed 
In-memory option was chosen due to the following estimations.

There are 4 significant fields (start, end, count, distance) that require at the very most 32 bytes. Per month data size may reach up to 15M records. Let's estimate data structures overhead as +100%. Then storage size estimation for year is 2 * 32 * 15M * 12 = 11.5Gb. Then a server with 256Gb RAM is enough to store 20 years of data.

## Implementation Idea
It's straightforward, [Interval Tree](https://en.wikipedia.org/wiki/Interval_tree) is an only data structure. `init()` phase creates separate interval tree for every possible `passenger_count` value. Every time interval in a tree contains `distance` value attached. For the query interval given Interval tree can provide a subset of intervals contained in the query. In turn, subset can be reduced to get the average distance. 

## Implementation Structure
Implementation class is `AverageDistancesImpl`, CSV loading uses [FastCSV](https://github.com/osiegmar/FastCSV), querying uses Interval Tree implementation from [Breinify](https://github.com/Breinify/brein-time-utilities/#intervaltree). `ManualTest` class can be used as a sandbox (works fine under Intellij Idea), `-Xmx` VM argument should be set to load real-size CSVs. For external usage, fat jar can be built with `./gradlew jar`.

## CSV Loading Algorithm
This builds a `map: passenger_count -> IntervalTree`. Records are processed sequentially, interval (with distance) is created for each record and gets to the corresponding Interval Tree. Performance of this part is out of scope. After loading, the trees stay immutable. 

## Querying Algorithm
It uses a thread pool of fixed-size (depends on available hardware threads). For the given query, the whole processing is split into jobs, one per `passenger_count` value. If the number of jobs are too small to utilize the thread pool, then they are taken into pieces by segmenting query interval.

## Known Flaws
Breinify `IntervalTree::overlapStream` operation creates a huge number of `java.util.stream`-related objects. This leads to very high memory consumption and significant GC pressure.

## Futher Tests and Measurements
- More unit tests
  - CSV parsing (even though it is third-party library)
  - Aggregation correctness
  - Sensitivity to query interval changes 
- Stress tests
  - High data volumes
  - Heavy (wide) queries
- Preformance benchmarking
- Memory usage benchmarking

## Further Research / Improvements
- Replace Interval Tree with own optimized implementation.
  - Manual memory management and proper job scheduling can theoretically use CPU caches efficiently.
- Optimized data import.

## Mistakes
- Almost re-invented Interval Tree on my own. And only then started to Google.
- Assumed it will be good to split Interval Trees to partitions corresponding to input CSV files. In practice, real files can contain interval from outside of their expected time interval.
