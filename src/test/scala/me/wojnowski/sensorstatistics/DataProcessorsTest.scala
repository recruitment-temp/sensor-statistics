package me.wojnowski.sensorstatistics

import cats.effect.ContextShift
import cats.effect.IO
import cats.scalatest.EitherValues
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
import cats.syntax.all._

class DataProcessorsTest extends AnyWordSpec with Matchers with EitherValues {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  "Summary calculator" when {
    "processing data" should {
      "correctly calculate min, avg and max" in {

        val entries = List(
          entry("1", "s1", 60),
          entry("1", "s1", 100),
          entry("1", "s1", 50)
        )

        val resultingSummaries =
          Stream
            .emits[IO, Entry](entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .unsafeRunSync()
            .flatMap(_.data.map { case (_, summary) => summary })

        resultingSummaries should contain theSameElementsAs List(
          summary(min = 50, avg = 70, max = 100)
        )
      }

      "not take NaNs into account" in {
        val entries = List(
          entry("1", "s1", Measurement.NaN),
          entry("1", "s1", 50),
          entry("1", "s1", Measurement.NaN),
          entry("1", "s1", Measurement.NaN)
        )

        val resultingSummaries =
          Stream
            .emits[IO, Entry](entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .unsafeRunSync()
            .flatMap(_.data.map { case (_, summary) => summary })

        resultingSummaries shouldBe List(
          summary(min = 50, avg = 50, max = 50)
        )
      }

      "return NaNs if all measurements were NaNs" in {
        val entries = List(entry("1", "s1", Measurement.NaN))

        val resultingSummaries =
          Stream
            .emits[IO, Entry](entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .unsafeRunSync()
            .flatMap(_.data.map { case (_, summary) => summary })

        resultingSummaries.head shouldBe SensorSummary(min = Measurement.NaN, avg = Measurement.NaN, max = Measurement.NaN)
      }

      "calculate data per sensor" in {
        val entries = new Random(0).shuffle(
          List(
            entry("1", "s1", 11),
            entry("1", "s1", 12),
            entry("1", "s1", 13),
            entry("1", "s1", 14),
            entry("1", "s1", 15),
            entry("1", "s2", 21),
            entry("1", "s2", 22),
            entry("1", "s2", 23),
            entry("1", "s2", 24),
            entry("1", "s2", 25),
            entry("1", "s3", 31),
            entry("1", "s3", 32),
            entry("1", "s3", 33),
            entry("1", "s3", 34),
            entry("1", "s3", 35)
          )
        )

        val resultingSummaries =
          Stream
            .emits[IO, Entry](entries)
            .through(DataProcessors.calculateSummaries)
            .compile
            .toList
            .unsafeRunSync()
            .flatMap(_.data)

        resultingSummaries should contain theSameElementsAs List(
          SensorId("s1") -> summary(
            min = 11,
            avg = 13,
            max = 15
          ),
          SensorId("s2") -> summary(
            min = 21,
            avg = 23,
            max = 25
          ),
          SensorId("s3") -> summary(
            min = 31,
            avg = 33,
            max = 35
          )
        )
      }
    }
  }

  "Summary sorter" when {
    "processing data" should {
      "sort summaries by average, descending" in {
        val summaries = List(
          SensorId("s1") -> summary(
            min = 10,
            avg = 40,
            max = 90
          ),
          SensorId("s2") -> summary(
            min = 10,
            avg = 50,
            max = 90
          ),
          SensorId("s3") -> summary(
            min = 10,
            avg = 60,
            max = 90
          )
        )

        val resultingAverages = Stream(Metric.ResultPerSensor(summaries))
          .through(DataProcessors.sortSummaries)
          .compile
          .toList
          .flatMap(_.data.map { case (_, summary) => summary.avg })

        resultingAverages should contain theSameElementsInOrderAs List(
          60,
          50,
          40
        )
      }

      "sort them, leaving NaNs at the end" in {
        val summaries = List(
          SensorId("s3") -> summary(
            min = 10,
            avg = 0,
            max = 90
          ),
          SensorId("s1") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.NaN, max = Measurement.Value(90)),
          SensorId("s2") -> summary(
            min = 10,
            avg = 100,
            max = 90
          ),
          SensorId("s3") -> SensorSummary(min = Measurement.Value(10), avg = Measurement.NaN, max = Measurement.Value(90))
        )

        val resultingAverages = Stream(Metric.ResultPerSensor(summaries))
          .through(DataProcessors.sortSummaries)
          .compile
          .toList
          .flatMap(_.data.map { case (_, summary) => summary.avg })

        resultingAverages should contain theSameElementsInOrderAs List(
          100,
          0,
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
          entry("1", "s3", Measurement.NaN),
          entry("2", "s2", 50),
          entry("3", "s1", Measurement.NaN),
          entry("4", "s1", 70),
          entry("4", "s1", 60)
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
          entry("1", "s3", Measurement.NaN),
          entry("2", "s2", 50),
          entry("2", "s1", Measurement.NaN),
          entry("3", "s1", 70),
          entry("3", "s1", 60)
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
          entry("1", "s3", Measurement.NaN),
          entry("2", "s2", 50),
          entry("2", "s1", Measurement.NaN),
          entry("3", "s1", 70),
          entry("3", "s1", 60)
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
          Metric.ResultPerSensor(
            List(
              SensorId("s1") -> summary(
                min = 60,
                avg = 65,
                max = 70
              ),
              SensorId("s2") -> summary(
                min = 50,
                avg = 50,
                max = 50
              ),
              SensorId("s3") -> SensorSummary(min = Measurement.NaN, avg = Measurement.NaN, max = Measurement.NaN)
            )
          )
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

  def entry(sourceId: String, sensorId: String, measurement: Measurement): Entry =
    (
      SourceId.validate(sourceId),
      SensorId.validate(sensorId)
    ).mapN(Entry(_, _, measurement)).value

  def entry(sourceId: String, sensorId: String, measurement: Int): Entry =
    entry(sourceId, sensorId, measurement)

  def summary(min: Int, avg: Int, max: Int): SensorSummary =
    (
      Measurement.ofInt(min),
      Measurement.ofInt(avg),
      Measurement.ofInt(max)
    ).mapN(SensorSummary.apply).value

}
