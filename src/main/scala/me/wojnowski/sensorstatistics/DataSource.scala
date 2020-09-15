package me.wojnowski.sensorstatistics

import fs2.Pipe
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.SourceId

trait DataSource[F[_]] {
  def streamData: Stream[F, Entry]
}

trait RawDataSource[F[_], A] {
  def streamRawData: Stream[F, A]
}

trait RawDataPreProcessor[F[_], A] {
  def preProcessPipe(sourceId: SourceId): Pipe[F, A, Entry]
}

