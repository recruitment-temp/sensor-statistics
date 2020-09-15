package me.wojnowski.sensorstatistics

import java.nio.file.Path

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.Sync
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.SourceId
import cats.syntax.all._
import fs2.Chunk
import fs2.Pipe

// TODO source ID is lost here :/
class DirectoryRawDataSource[F[_]: Sync: ContextShift](directory: Path, blocker: Blocker, chunkSize: Int) extends RawDataSource[F, Char] {

  override def streamRawData: fs2.Stream[F, Char] =
    streamFilePaths.flatMap { path =>
      Stream
        .eval(pathToSourceId(path))
        .flatMap(sourceId => readFile(path))
        .through(stringToCharPipe)
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

  private def stringToCharPipe: Pipe[F, String, Char] =
    _.mapChunks(_.flatMap(x => Chunk.array(x.toCharArray)))

}
