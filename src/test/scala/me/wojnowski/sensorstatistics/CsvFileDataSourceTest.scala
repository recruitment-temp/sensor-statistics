package me.wojnowski.sensorstatistics

import java.nio.file.Paths

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.IO
import me.wojnowski.sensorstatistics.domain.SourceId
import org.scalatest.freespec.AnyFreeSpec
import eu.timepit.refined.auto._
import fs2.Chunk
import fs2.Pipe
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.SensorId
import org.scalatest.matchers.should.Matchers

class CsvFileDataSourceTest extends AnyWordSpec with Matchers with CommonTestValues {
  val blocker = Blocker.liftExecutionContext(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val dataSource = new CsvDataPreProcessor[IO]

  "CsvFileDataSource" should {
    "skip first, non-empty line" in {
      val data =
        """header,line
          |s1,50
          |s2,NaN""".stripMargin

      val resultingEntries =
        Stream
          .emit(data)
          .through(dataSource.preProcessPipe(SourceId1))
          .compile
          .toList
          .unsafeRunSync()

      resultingEntries shouldBe List(
        Entry(SourceId1, SensorId("s1"), Measurement.Value(50)),
        Entry(SourceId1, SensorId("s2"), Measurement.NaN)
      )
    }

    "not require data to be chunked in any particular way (e.g. by line)" in {
      val data =
        """h!eade!r,li!ne
          |s!1,!5!0"""
          .stripMargin
          .split('!')

      val resultingEntries =
        Stream
          .emits(data)
          .debug()
          .through(dataSource.preProcessPipe(SourceId1))
          .compile
          .toList
          .unsafeRunSync()

      resultingEntries shouldBe List(
        Entry(SourceId1, SensorId("s1"), Measurement.Value(50))
      )
    }

    "skip empty lines" in {
      val data =
        """
          |header,line
          |
          |s1,50
          |""".stripMargin

      val resultingEntries =
        Stream
          .emit(data)
          .through(dataSource.preProcessPipe(SourceId1))
          .compile
          .toList
          .unsafeRunSync()

      resultingEntries shouldBe List(
        Entry(SourceId1, SensorId("s1"), Measurement.Value(50))
      )
    }

    "skip lines with invalid number of arguments" in {
      val data =
        """
          |header,line
          |s1
          |s1,50
          |foo,bar,baz
          |s2,30
          |""".stripMargin

      val resultingEntries =
        Stream
          .emit(data)
          .through(dataSource.preProcessPipe(SourceId1))
          .compile
          .toList
          .unsafeRunSync()

      resultingEntries shouldBe List(
        Entry(SourceId1, SensorId("s1"), Measurement.Value(50)),
        Entry(SourceId1, SensorId("s2"), Measurement.Value(30))
      )
    }

    "skip lines with invalid data (sensor ID, measurement value out of range)" in {
      val data =
        """
          |header,line
          |s1,-1
          |s2,0
          |s3,101
          |,30
          |s4,100
          |""".stripMargin

      val resultingEntries =
        Stream
          .emit(data)
          .through(dataSource.preProcessPipe(SourceId1))
          .compile
          .toList
          .unsafeRunSync()

      resultingEntries shouldBe List(
        Entry(SourceId1, SensorId("s2"), Measurement.Value(0)),
        Entry(SourceId1, SensorId("s4"), Measurement.Value(100))
      )
    }
  }

}
