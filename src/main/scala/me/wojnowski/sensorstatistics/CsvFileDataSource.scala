package me.wojnowski.sensorstatistics

import java.nio.file.Path

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.Sync
import fs2.Chunk
import me.wojnowski.sensorstatistics.domain.Entry
import fs2.Stream
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.SensorId
import me.wojnowski.sensorstatistics.domain.SourceId
import fs2.Pipe

// TODO refactor
class CsvFileDataSource[F[_]: Sync: ContextShift](
  directory: Path,
  blocker: Blocker,
  chunkSize: Int,
  rawDataPreProcessor: RawDataPreProcessor[F, String]
) extends DataSource[F] {

  implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]

  // TODO name?
  def csvProcessingPipe(sourceId: SourceId): Pipe[F, String, Entry] =
    _.through(rawDataPreProcessor.preProcessPipe(sourceId))

  override def streamData: fs2.Stream[F, Entry] =
    streamFilePaths.flatMap { path =>
      Stream
        .eval(pathToSourceId(path))
        .flatMap(sourceId => readFile(path).through(csvProcessingPipe(sourceId)))
    }

  private def readFile(path: Path): Stream[F, String] =
    fs2
      .io
      .file
      .readAll(path, blocker, chunkSize)
      .through(fs2.text.utf8Decode)

  private def pathToSourceId(path: Path): F[SourceId] =
    Sync[F].fromEither(
      SourceId
        .validate(path.getFileName.toString)
        .leftMap(message => new IllegalArgumentException(s"Error during source ID creation: $message"))
    )

  private def streamFilePaths: Stream[F, Path] =
    fs2
      .io
      .file
      .directoryStream[F](blocker, directory)

}
