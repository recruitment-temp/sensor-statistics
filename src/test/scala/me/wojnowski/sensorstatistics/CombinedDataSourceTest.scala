package me.wojnowski.sensorstatistics

import cats.effect.ContextShift
import cats.effect.IO
import fs2.Pipe
import org.scalatest.wordspec.AnyWordSpec
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.SensorId
import me.wojnowski.sensorstatistics.domain.SourceId
import eu.timepit.refined.auto._
import me.wojnowski.sensorstatistics.domain.Measurement
import cats.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class CombinedDataSourceTest extends AnyWordSpec with CommonTestValues with Matchers with EitherValues {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  "Combined DataSource" should {
    "pass data from RawDataSource through RawDataPreProcessor" in {
      val rawDataSource: RawDataSource[IO, Int] = new RawDataSource[IO, Int] {
        override def streamRawData: Stream[IO, (SourceId, Stream[IO, Int])] =
          Stream(
            SourceId1 -> Stream.range[IO](0, 5),
            SourceId2 -> Stream.range[IO](5, 10)
          )
      }

      val rawDataPreProcessor: RawDataPreProcessor[IO, Int] = new RawDataPreProcessor[IO, Int] {
        override def preProcessPipe(sourceId: SourceId): Pipe[IO, Int, Entry] =
          _.map { x =>
            if (x % 2 == 0) Entry(sourceId, SensorId("even-sensor"), Measurement.ofInt(x).value)
            else Entry(sourceId, SensorId("odd-sensor"), Measurement.ofInt(x).value)
          }
      }

      val resultingEntries =
        DataSource
          .fromSourceAndPreProcessor(rawDataSource, rawDataPreProcessor)
          .streamData
          .compile
          .toList
          .unsafeRunSync()

      resultingEntries should contain theSameElementsAs List(
        entry(SourceId1.value, "even-sensor", 0),
        entry(SourceId1.value, "even-sensor", 2),
        entry(SourceId1.value, "even-sensor", 4),
        entry(SourceId2.value, "even-sensor", 6),
        entry(SourceId2.value, "even-sensor", 8),
        entry(SourceId1.value, "odd-sensor", 1),
        entry(SourceId1.value, "odd-sensor", 3),
        entry(SourceId2.value, "odd-sensor", 5),
        entry(SourceId2.value, "odd-sensor", 7),
        entry(SourceId2.value, "odd-sensor", 9)
      )
    }
  }

}
