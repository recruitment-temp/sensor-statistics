package me.wojnowski.sensorstatistics

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.Executors

import cats.effect.Blocker
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Sync
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.util.Try
import scala.util.control.NoStackTrace

object Main extends IOApp {
  val MaxParallelFiles = 4
  val ChunkSize = 4096

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val blocker = Blocker.liftExecutorService(Executors.newCachedThreadPool())

  override def run(arguments: List[String]): IO[ExitCode] =
    validateArguments[IO](arguments)
      .flatTap(path => Logger[IO].info(s"Reading data from [$path]..."))
      .flatTap { path =>
        new CsvFileDataSource[IO](
          path,
          blocker,
          chunkSize = ChunkSize,
          maxParallelFiles = MaxParallelFiles,
          new CsvDataPreProcessor[IO]
        )
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
        case throwable                          =>
          Logger[IO].error(s"Unknown error occurred: $throwable").as(ExitCode.Error)
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
