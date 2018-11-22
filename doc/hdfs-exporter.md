# Purpose
Garmadon events are collected on workers, then pushed to a Kafka topic. From there, we need to export them to HDFS for history and perform complex queries. These data will notably feed Hadoop metrology.

# Constraints
By design:
* All event types are mixed into the same Kafka topic
* All events have different schemas
* Events might be out of order in Kafka since we consider their generation date, not when they got into Kafka

By choice:
* The output format is Parquet
* The export tool runs on Mesos and is therefore stateless
* Events are partitioned per day and type

# Design
Due to above constraints, we need to:
* Export events to different files based on their type
* Commit offsets to recover after a restart or a crash

# Output
At any point in time, there is at most one writer/file open for a given _(event type, Kafka partition)_ tuple. All such events are written to the same temporary file. When they hold enough events (or based on time, cf below), they are moved to their final destination.

## Offsets
Since events are mixed in the same Kafka partition, one can commit a given offset if and only if all events have been written to their final destination.
Known technical issues & constraints
* Partition rebalancing: A given writer might get its work "stolen" by another one when Kafka decides to reshuffle partitions for writers.
* Duplicate events in case of restarts: No 2 files should hold the same events.
* Non-deterministic files writing: Given that the rules for writing a file are not deterministic (eg. time-based, transient failures), one cannot assume that two runs of the same app on the same events will create the same files.
* Days overlap: The system should be resilient to out-of-order events overlapping different days.

## Tech-agnostic behavior
* Let A, B, C be distinct event types and On global offsets.
* "XXX" are timeframes during which writing fails for a given writer.
* "---" are events read from Kafka.
* "|" is a successful write to the final destination.

The following examples only discuss global offsets, as per-event offsets are implementation details specific to where we "commit offsets".

### Happy cases
Assume writing events to their final destination works seamlessly all the time.

          t1  t2    t3  t4       t5
    -------------------------------->
    A ----|
         O1
    B         |------  --|
             O2          O4
    C               |--   -------|
                   O3           O5

#### Offsets
* At t1, only A has unwritten events. O1 gets committed.
* At t2, only B has unwritten events. No offset gets committed.
* At t3, both B and C have unwritten events. No offset gets committed.
* At t4, both B and C have unwritten events. O3 gets committed as all B events before that offset were committed. No further offset can be committed as C events are not written yet.
* At t5, only C has unwritten events. O5 gets committed.

### Unhappy cases

          t1  t2  t3   t4  t5    t6		  t7
    ------------------------------------------->
    A ----XXXXXXXXX|-  -|
         O1        O3   O4
    B         |------  -------- -|
             O2                 O6
    C                      |-- -  --------|
                          O5              O7

#### Offsets
* At t1, ony A has unwritten events because it fails writing them. No offset gets committed.
* At t2, both A and B have unwritten events. No offset gets committed.
* At t3, only B has unwritten events. O2 gets committed as B events after O2 are not written yet.
* At t4, only B has unwritten events. No offsets gets committed.
* At t5, both B and C have unwritten events. No offset gets committed.
* At t6, only B has unwritten events. O5 gets committed.
* At t7, only C has unwritten events. O7 gets committed.

### Dealing with duplicate events
There are 2 possible ways to get duplicate events:
* The app restarts and resumes from the latest offset. Some events might already be persisted to disk without the global offset changing. We need to be able to overwrite, replace or not rewrite these seamlessly
* Input partitions change owners because of rebalancing

### Rebalancing
Let:
* C1 and C2 be 2 consumers of the same partition PA
* O1, O2...On offsets in the input topic

          t0        t1    t2      t3
      ---------------------------------->
      C1    |---------       |
            O0               O1
      C2               |--   ------|
                       O0          O2
                       |----|
                      O0    O2

At t1, C1 stops receiving events for PA at the same time as C2 starts receiving them

Most of the time, C1 should expire before C2. The other way around is still possible though, in case these have different timeouts or any writing issue occurs. Let's then consider the 2 scenarios displayed above:
* O1 < O2: Duplicate events between O0 and O1
* O1 > O2: Duplicate events between O0 and O2

#### App restart
Let:
* F be a failing and S a successful consumer
* O1...On be strictly ascending offsets

First run:

          t0     t1    t2
    ----------------------->
    F   | --XXXXXXXXXXXX
        O0
    S   |-  -----|-----|
        O0       O1    O3

* S successfully wrote events from O0 to O1 and from O1 to O3 in 2 separate files
* Since F always failed writing, the overall offset is still O0

Second run:

       t0  t1    t2    t3
    ----------------------->
    F   |---|
        O0  O1
    S1  |--  ----|-----|
        O0       O2    O4

* S writes the same event as previously, but with potentially different offsets
* F writes non-duplicate events

#### Time-based partitioning
* Input events are out-of-order in terms of their effective timestamp, since they are tagged with their *generation time* and not their *insertion time* in Kafka
* At any point in time, but more specifically around end of the day, an event from day D might occur after some from day D + 1; In case of restart, the latest offset is possibly not the one from the latest day, eg.

         t0  t1    t2   t3  t4 t5   t6
      -------------------------------->
      D0                       |----|
                               O5   O6
      D1 |---- ----| -------|
         O0        O2       O4
      D2     |-     ----|
             O1         O3

| Time | Global offset | Taken from
| ---- | ------------- | ---------- |
|  t0  |       O0      |     D1     |
|  t1  |       O0      |     D1     |
|  t2  |       O1      |     D2     |
|  t3  |       O2      |     D1     |
|  t4  |       O4      |     D1     |
|  t5  |       O5      |     D0     |
|  t6  |       O6      |     D0     |

#### Sparse events
Events may vary greatly in cardinality and frequency, therefore:
* To prevent holding forever onto open files, files also get written to their final destination after a given number of minutes (set fairly high, mostly as a safeguard)
* To prevent resuming from very old offsets, we need to make sure empty event types are considered as such. We cannot consider an empty directory or a very old offset as an acknowledgment that there was no event in between, as there may have been a temporary issue with writing

#### Conclusion
* We cannot rely on the output files including the same events/offsets in any number of distinct runs
* We cannot rely on consistent behaviors in case of rebalancing and therefore need to consider that
** Offsets could be different (or equal)
** Readers which got unassigned from the partition might write its final output later than the newly-assigned one (and vice-versa)
* When performing time-based partitioning (day, hour, ...), offset cannot always be taken from the latest day. Worst-case scenario, the offset might be coming from a few days ago

## Implementation
Offsets are stored in HDFS output files names:
* Files naming: File names template is _<day>/<partition_id>.<last_file_offset>_
* Writing files to the final destination:
** Every E events or M minutes, the temporary file gets moved to its final destination
** Every few minutes a heartbeat is sent to all writers
*** If they don't have any pending writes, they will create an empty file suffixed by the latest global offset
*** This way, in case of restart or reshufffle, the event type will be considered as up-to-date
* At startup:
** For each _(event type, partition)_, read latest offsets from final directory (all available days if relevant), based on file names
** For each _(event type, partition)_, skip all events with offset < latest_offset (they should already be persisted)
** Start from _min(latest offsets)_
* Rebalancing:
** Because we use Kafka's KafkaConsumer class, we can rely on the "partitions revoked" callback to be called on the deprecated owner to be called before any event is sent to the new consumer
** When such a rebalancing occurs, we notify the writers for this partition to drop whatever they were doing, thereby avoiding to write duplicate events

## Considered alternatives
The main alternative is to use Kafka to store the global offset. This would mean:
* At startup
** Start from offset in Kafka
** Delete all files such that filename = _<current_partition>.<offset>_, where _offset >= Kafka's offset_
* When ready to write to HDFS
** Write file to HDFS
** Commit _min(latest offsets)_ for all event types

Main caveats are:
* Not atomic (writing the actual file vs the offset)
* Would rewrite already written events and require deleting existing files

# Monitoring
Considering _latest committed_ and _latest commitable_ (stored in temporary file, but not moved to final destination) offsets for all event types for a given partition
* Percentage of writes failures (to final destination) < XX%
* Delay between _max(latest committable)_ and _latest Kafka offset_ < XX
* Discrepancy between events' _latest committed_ offsets <= E events (+ delta)
** For a given event, if _latest committed = latest commitable_, then consider it to be up-to-date
** Otherwise, consider _max(latest committed) - min(latest committed)_
