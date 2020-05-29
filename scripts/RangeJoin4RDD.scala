import java.sql.Timestamp

implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    // to make descendent sort order
    def compare(x: Timestamp, y: Timestamp): Int = y compareTo x
}

case class ProcData(proc_id: String, begin_ts: java.sql.Timestamp, end_ts: java.sql.Timestamp,
                    ts: java.sql.Timestamp)

case class JoinData(common_ts: java.sql.Timestamp, proc_id: String, begin_ts: java.sql.Timestamp, end_ts: java.sql.Timestamp,
                    ts: java.sql.Timestamp)
object JoinData {
    def apply(cts: java.sql.Timestamp, h: HeatData) = 
        new JoinData(cts, h.proc_id, h.begin_ts, h.end_ts, h.ts)
}

val joined_df = ...

val join_rdd = joined_df.select($"common_ts", $"proc_id", $"begin_ts", $"end_ts", $"ts")
  .as[JoinData]
  .rdd
  .map(r => (r.common_ts, HeatData(r.proc_id, r.begin_ts, r.end_ts, r.ts)))
  .persist

val timePartitioner = new RangePartitioner(200, part_ts)
val trends_parted = join_rdd.repartitionAndSortWithinPartitions(timePartitioner).persist

val dum1 = HeatData("-1", Timestamp.valueOf("1970-01-01 00:00:00"),
                    Timestamp.valueOf("1970-01-01 00:00:00"),
                    Timestamp.valueOf("1970-01-01 00:00:00"))

// Buffer for previous value in each partition
var buffer = trends_parted.partitions.map(_.index -> dum1).toMap

val trends_joined = trends_parted.mapPartitionsWithIndex({ (idx, iterator) =>
    iterator.toList.map{_ match {
        // if NULL fill with value from buffer
        case (cts: java.sql.Timestamp, h: HeatData) if Option(h.proc_end_ts).isEmpty => 
            (cts, buffer.getOrElse(idx, dum1))
        // if not update leave the value as it is and buffer
        case (cts: java.sql.Timestamp, h: HeatData) if !Option(h.proc_end_ts).isEmpty => {
            // update buffer
            buffer = buffer map {
                case (i, _) if (i == idx) => idx -> h
                case r => r
            }
            (cts, h)
        }
        case _ => throw new NoSuchElementException
    }}.iterator}, false).persist
