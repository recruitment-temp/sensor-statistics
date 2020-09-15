package me.wojnowski.sensorstatistics

import java.nio.file.Path

import cats.effect.Blocker
import cats.effect.Concurrent
import cats.effect.ContextShift
import cats.effect.Sync
import cats.syntax.all._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.SourceId

class CsvFileDataSource[F[_]: Concurrent: ContextShift](
  directory: Path,
  blocker: Blocker,
  chunkSize: Int,
  maxParallelFiles: Int,
  rawDataPreProcessor: RawDataPreProcessor[F, String]
) extends DataSource[F] {

  implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def streamData: fs2.Stream[F, Entry] =
    streamFilePaths
      .map { path =>
        Stream
          .eval(Logger[F].info(s"Reading CSV file [$path]..."))
          .evalMap(_ => pathToSourceId(path))
          .flatMap(sourceId => readFile(path).through(rawDataPreProcessor.preProcessPipe(sourceId)))
      }
      .parJoin(maxParallelFiles)

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
