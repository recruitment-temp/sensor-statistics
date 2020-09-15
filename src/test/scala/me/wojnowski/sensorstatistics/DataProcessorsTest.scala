package me.wojnowski.sensorstatistics

import cats.effect.ContextShift
import cats.effect.IO
import org.scalatest.wordspec.AnyWordSpec
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.SensorSummary
import eu.timepit.refined.auto._
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Metric
import me.wojnowski.sensorstatistics.domain.SensorId
import me.wojnowski.sensorstatistics.domain.SourceId
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.util.Random

// TODO Decrease compile times by limiting usage of refined macros
class DataProcessorsTest extends AnyWordSpec with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  // TODO how about some property-based testing?
  "Summary calculator" when {
    "processing data" should {
      "correctly calculate min, avg and max" in {
        val entries = List(
          Entry(SourceId("1"), SensorId("s1"), Measurement.Value(60)),
          Entry(SourceId("1"), SensorId("s1"), Measurement.Value(100)),
          Entry(SourceId("1"), SensorId("s1"), Measurement.Value(50))
        )

        val resultingSummaries =
          Stream
            .emits(entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .flatMap(_.data.map { case (_, summary) => summary })

        resultingSummaries should contain theSameElementsAs List(
          SensorSummary(min = Measurement.Value(50), avg = Measurement.Value(70), max = Measurement.Value(100))
        )
      }

      "not take NaNs into account" in {
        val entries = List(
          Entry(SourceId("1"), SensorId("s1"), Measurement.NaN),
          Entry(SourceId("1"), SensorId("s1"), Measurement.Value(50)),
          Entry(SourceId("1"), SensorId("s1"), Measurement.NaN),
          Entry(SourceId("1"), SensorId("s1"), Measurement.NaN)
        )

        val resultingSummaries =
          Stream
            .emits(entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .flatMap(_.data.map { case (_, summary) => summary })

        resultingSummaries shouldBe SensorSummary(min = Measurement.Value(50), avg = Measurement.Value(50), max = Measurement.Value(50))
      }

      "return NaNs if all measurements were NaNs" in {
        val entry = Entry(SourceId("1"), SensorId("s1"), Measurement.NaN)

        val resultingSummaries =
          Stream
            .emit(entry)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .flatMap(_.data.map { case (_, summary) => summary })

        resultingSummaries.head shouldBe SensorSummary(min = Measurement.NaN, avg = Measurement.NaN, max = Measurement.NaN)
      }

      "calculate data per sensor" in {
        val entries = new Random(0).shuffle(
          List(
            Entry(SourceId("1"), SensorId("s1"), Measurement.Value(11)),
            Entry(SourceId("1"), SensorId("s1"), Measurement.Value(12)),
            Entry(SourceId("1"), SensorId("s1"), Measurement.Value(13)),
            Entry(SourceId("1"), SensorId("s1"), Measurement.Value(14)),
            Entry(SourceId("1"), SensorId("s1"), Measurement.Value(15)),
            Entry(SourceId("1"), SensorId("s2"), Measurement.Value(21)),
            Entry(SourceId("1"), SensorId("s2"), Measurement.Value(22)),
            Entry(SourceId("1"), SensorId("s2"), Measurement.Value(23)),
            Entry(SourceId("1"), SensorId("s2"), Measurement.Value(24)),
            Entry(SourceId("1"), SensorId("s2"), Measurement.Value(25)),
            Entry(SourceId("1"), SensorId("s3"), Measurement.Value(31)),
            Entry(SourceId("1"), SensorId("s3"), Measurement.Value(32)),
            Entry(SourceId("1"), SensorId("s3"), Measurement.Value(33)),
            Entry(SourceId("1"), SensorId("s3"), Measurement.Value(34)),
            Entry(SourceId("1"), SensorId("s3"), Measurement.Value(35))
          )
        )

        val resultingSummaries =
          Stream
            .emits(entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .flatMap(_.data)

        resultingSummaries should contain theSameElementsAs List(
          SensorId("s1") -> SensorSummary(min = Measurement.Value(11), avg = Measurement.Value(13), max = Measurement.Value(15)),
          SensorId("s2") -> SensorSummary(min = Measurement.Value(21), avg = Measurement.Value(23), max = Measurement.Value(25)),
          SensorId("s3") -> SensorSummary(min = Measurement.Value(31), avg = Measurement.Value(33), max = Measurement.Value(35))
        )
      }
    }
  }

  "Summary sorter" when {
    "processing data" should {
      "sort summaries by average, descending" in {
        val summaries = List(
          SensorId("s1") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.Value(40), max = Measurement.Value(90)),
          SensorId("s2") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.Value(50), max = Measurement.Value(90)),
          SensorId("s3") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.Value(60), max = Measurement.Value(90))
        )

        val resultingAverages = Stream(Metric.ResultPerSensor(summaries))
          .through(DataProcessors.sortSummaries)
          .compile
          .toList
          .flatMap(_.data.map { case (_, summary) => summary.avg })

        resultingAverages should contain theSameElementsInOrderAs List(
          Measurement.Value(60),
          Measurement.Value(50),
          Measurement.Value(40)
        )
      }

      "sort them, leaving NaNs at the end" in {
        val summaries = List(
          SensorId("s3") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.Value(0), max = Measurement.Value(90)),
          SensorId("s1") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.NaN, max = Measurement.Value(90)),
          SensorId("s2") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.Value(100), max = Measurement.Value(90)),
          SensorId("s3") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.NaN, max = Measurement.Value(90))
        )

        val resultingAverages = Stream(Metric.ResultPerSensor(summaries))
          .through(DataProcessors.sortSummaries)
          .compile
          .toList
          .flatMap(_.data.map { case (_, summary) => summary.avg })

        resultingAverages should contain theSameElementsInOrderAs List(
          Measurement.Value(100),
          Measurement.Value(0),
          Measurement.NaN,
          Measurement.NaN
        )
      }
    }
  }

  "Measurement counter" when {
    "processing data" should {
      "count successes and failures regardless of source and sensor" in {
        val entries = List(
          Entry(SourceId("1"), SensorId("s3"), Measurement.NaN),
          Entry(SourceId("2"), SensorId("s2"), Measurement.Value(50)),
          Entry(SourceId("3"), SensorId("s1"), Measurement.NaN),
          Entry(SourceId("4"), SensorId("s1"), Measurement.Value(70)),
          Entry(SourceId("4"), SensorId("s1"), Measurement.Value(60))
        )

        val resultingMetrics: List[Metric] =
          Stream
            .emits(entries)
            .through(DataProcessors.countMeasurements)
            .compile
            .toList

        resultingMetrics should contain theSameElementsAs List(
          Metric.MeasurementsProcessed(successful = 3, failed = 2)
        )
      }

    }

    "return zeroes for no entries" in {
      val entries = List.empty[Entry]

      val resultingMetrics: List[Metric] =
        Stream
          .emits(entries)
          .through(DataProcessors.countMeasurements)
          .compile
          .toList

      resultingMetrics should contain theSameElementsAs List(
        Metric.MeasurementsProcessed(successful = 0, failed = 0)
      )
    }
  }

  "File counter" when {
    "processing non-empty data" should {
      "count files" in {
        val entries = List(
          Entry(SourceId("1"), SensorId("s3"), Measurement.NaN),
          Entry(SourceId("2"), SensorId("s2"), Measurement.Value(50)),
          Entry(SourceId("2"), SensorId("s1"), Measurement.NaN),
          Entry(SourceId("3"), SensorId("s1"), Measurement.Value(70)),
          Entry(SourceId("3"), SensorId("s1"), Measurement.Value(60))
        )

        val resultingMetrics: List[Metric] =
          Stream
            .emits(entries)
            .through(DataProcessors.countFiles)
            .compile
            .toList

        resultingMetrics should contain theSameElementsAs List(
          Metric.ProcessedFiles(3)
        )
      }

    }

    "count files without entries" in {
      /*val entries = List.empty[Entry]

      val resultingMetrics: List[Metric] =
        Stream
          .emits(entries)
          .through(DataProcessors.countMeasurements)
          .compile
          .toList

      resultingMetrics should contain theSameElementsAs List(
        Metric.MeasurementsProcessed(successful = 0, failed = 0),
      )*/

      // TODO that won't work :/
      fail()
    }
  }

  "processing zero files" should {
    "return zero" in {
      val entries = List.empty[Entry]

      val resultingMetrics: List[Metric] =
        Stream
          .emits(entries)
          .through(DataProcessors.countFiles)
          .compile
          .toList

      resultingMetrics should contain theSameElementsAs List(
        Metric.ProcessedFiles(0)
      )
    }
  }

  "aggregate processor" when {
    "processing some files" should {
      "return all metrics, including sorted results per sensor" in {
        val entries = List(
          Entry(SourceId("1"), SensorId("s3"), Measurement.NaN),
          Entry(SourceId("2"), SensorId("s2"), Measurement.Value(50)),
          Entry(SourceId("2"), SensorId("s1"), Measurement.NaN),
          Entry(SourceId("3"), SensorId("s1"), Measurement.Value(70)),
          Entry(SourceId("3"), SensorId("s1"), Measurement.Value(60))
        )

        val resultingMetrics: List[Metric] =
          Stream
            .emits[IO, Entry](entries)
            .through(DataProcessors.aggregate[IO])
            .compile
            .toList
            .unsafeRunSync()

        resultingMetrics should contain theSameElementsAs List(
          Metric.ProcessedFiles(3),
          Metric.MeasurementsProcessed(successful = 3, failed = 2),
          Metric.ResultPerSensor(List(
            SensorId("s1") -> SensorSummary(min = Measurement.Value(60), avg = Measurement.Value(65), max = Measurement.Value(70)),
            SensorId("s2") -> SensorSummary(min = Measurement.Value(50), avg = Measurement.Value(50), max = Measurement.Value(50)),
            SensorId("s3") -> SensorSummary(min = Measurement.NaN, avg = Measurement.NaN, max = Measurement.NaN)
          ))
        )
      }
    }

    "processing zero files" should {
      "return all metrics" in {
        val entries = List.empty[Entry]

        val resultingMetrics: List[Metric] =
          Stream
            .emits[IO, Entry](entries)
            .through(DataProcessors.aggregate[IO])
            .compile
            .toList
            .unsafeRunSync()

        resultingMetrics should contain theSameElementsAs List(
          Metric.ProcessedFiles(0),
          Metric.MeasurementsProcessed(successful = 0, failed = 0),
          Metric.ResultPerSensor(List.empty)
        )
      }
    }
  }
}
