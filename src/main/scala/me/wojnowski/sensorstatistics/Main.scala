package me.wojnowski.sensorstatistics

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

import cats.data.Chain
import cats.effect.Blocker
import cats.effect.Concurrent
import cats.effect.ContextShift
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Sync
import fs2.Stream
import fs2.Pipe

import scala.concurrent.ExecutionContext
import scala.util.Try
import cats.syntax.all._
import fs2.io.file.FileHandle
import fs2.io.file.readAll
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Metric
import me.wojnowski.sensorstatistics.domain.SourceId
import cats.effect.implicits._
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.SensorId

import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

/* TODO
 * Open questions:
 * - the order of metrics
 * - Logging!
 * - test compile times
 * - refactor main and larger functions
 * */
object Main extends IOApp {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val blocker = Blocker.liftExecutorService(Executors.newCachedThreadPool())

  override def run(arguments: List[String]): IO[ExitCode] =
    validateArguments[IO](arguments)
      .flatTap(path => Logger[IO].info(s"Reading data from [$path]..."))
      .flatTap { path =>
        new CsvFileDataSource[IO](path, blocker, chunkSize = 4096, new CsvDataPreProcessor[IO]) // TODO config
          .streamData
          .through(DataProcessors.aggregate)
          .evalTap(metric => IO(println(metric.show)))
          .compile
          .drain
      }
      .flatTap(path => Logger[IO].info(s"Read all data from [$path]."))
      .as(ExitCode.Success)
      .recoverWith {
        case IncorrectProgramArguments(message) =>
          Logger[IO].error(s"Error: $message").as(ExitCode.Error)
        case NonFatal(t)                        =>
          Logger[IO].error(s"Unknown error occurred: $t").as(ExitCode.Error)
      }

  private def validateArguments[F[_]: Sync](arguments: List[String]): F[Path] =
    arguments match {
      case List(rawPath) =>
        Sync[F]
          .fromTry(Try(Paths.get(rawPath)))
          .flatTap { path =>
            Sync[F]
              .delay(Files.isDirectory(path))
              .ifM(
                ().pure[F],
                IncorrectProgramArguments(s"Supplied path [$path] doesn't exist or is not a directory").raiseError[F, Unit]
              )
          }
      case args          =>
        IncorrectProgramArguments(s"Incorrect arguments. Expected single path to a directory, provided: [$args]").raiseError[F, Path]
    }

  case class IncorrectProgramArguments(message: String) extends NoStackTrace

}
