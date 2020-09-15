package me.wojnowski.sensorstatistics

import cats.effect.Concurrent
import fs2.Pipe
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.SourceId

trait DataSource[F[_]] {
  def streamData: Stream[F, Entry]
}

object DataSource {

  def fromSourceAndPreProcessor[F[_]: Concurrent, A](
    rawDataSource: RawDataSource[F, A],
    rawDataPreProcessor: RawDataPreProcessor[F, A],
    maxParallelSources: Int = Int.MaxValue
  ): DataSource[F] =
    new DataSource[F] {

      override def streamData: Stream[F, Entry] =
        rawDataSource
          .streamRawData
          .map {
            case (sourceId, dataStream) => dataStream.through(rawDataPreProcessor.preProcessPipe(sourceId))
          }
          .parJoin(maxParallelSources)

    }

}

trait RawDataSource[F[_], A] {
  def streamRawData: Stream[F, (SourceId, Stream[F, A])]
}

trait RawDataPreProcessor[F[_], A] {
  def preProcessPipe(sourceId: SourceId): Pipe[F, A, Entry]
}
