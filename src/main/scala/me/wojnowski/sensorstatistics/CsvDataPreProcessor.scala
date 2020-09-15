package me.wojnowski.sensorstatistics

import cats.effect.Sync
import fs2.Pipe
import fs2.Stream
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.SensorId
import cats.syntax.all._
import fs2.Chunk
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import me.wojnowski.sensorstatistics.domain.SourceId

class CsvDataPreProcessor[F[_]: Sync] extends RawDataPreProcessor[F, String] {
  implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def preProcessPipe(sourceId: SourceId): Pipe[F, String, Entry] =
    _.through(stringToCharPipe)
      .through(fs2.data.csv.rows[F]())
      .drop(1)
      .map(_.toList)
      .flatMap {
        case sensorId :: result :: Nil =>
          (
            SensorId.validate(sensorId),
            Measurement.parseString(result)
          ).tupled match {
            case Right((sensorId, result)) =>
              Stream.emit(Entry(sourceId, sensorId, result))
            case Left(errorMessage)        =>
              Stream.eval(logger.warn(s"Couldn't parse data [$sensorId, $result], details: $errorMessage")).drain
          }
        case columns                   =>
          Stream.eval(logger.warn(s"Incorrect number of columns, tried to parse [$columns]")).drain
      }

  private def stringToCharPipe: Pipe[F, String, Char] =
    _.mapChunks(_.flatMap(x => Chunk.array(x.toCharArray)))
}
