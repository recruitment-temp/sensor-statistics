package me.wojnowski.sensorstatistics

import java.nio.file.Path

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.Sync
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import me.wojnowski.sensorstatistics.domain.SourceId
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class FileDataSource[F[_]: Sync: ContextShift](
  directory: Path,
  chunkSize: Int,
  blocker: Blocker
) extends RawDataSource[F, String] {

  implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def streamRawData: fs2.Stream[F, (SourceId, fs2.Stream[F, String])] =
    streamFilePaths
      .flatMap { path =>
        Stream
          .eval(Logger[F].info(s"Reading CSV file [$path]..."))
          .flatMap(_ => Stream.eval(pathToSourceId(path)).map((_, readFile(path))))
      }

  private def streamFilePaths: Stream[F, Path] =
    fs2
      .io
      .file
      .directoryStream[F](blocker, directory)

  private def pathToSourceId(path: Path): F[SourceId] =
    Sync[F].fromEither(
      SourceId
        .validate(path.getFileName.toString)
        .leftMap(message => new IllegalArgumentException(s"Error during source ID creation: $message"))
    )

  private def readFile(path: Path): Stream[F, String] =
    fs2
      .io
      .file
      .readAll(path, blocker, chunkSize)
      .through(fs2.text.utf8Decode)

}
