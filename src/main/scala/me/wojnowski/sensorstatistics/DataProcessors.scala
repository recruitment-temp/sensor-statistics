package me.wojnowski.sensorstatistics

import cats.effect.Concurrent
import cats.effect.Sync
import fs2.Pipe
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.Metric
import me.wojnowski.sensorstatistics.domain.Metric.MeasurementsProcessed
import me.wojnowski.sensorstatistics.domain.SensorId
import me.wojnowski.sensorstatistics.domain.SensorSummary
import me.wojnowski.sensorstatistics.domain.SourceId
import cats.syntax.all._

import scala.util.Success
import scala.util.Try

object DataProcessors {

  def aggregate[F[_]: Concurrent]: Pipe[F, Entry, Metric] =
    _.broadcastThrough(
      DataProcessors.countFiles[F],
      DataProcessors.countMeasurements[F],
      DataProcessors.calculateSummaries[F] andThen DataProcessors.sortSummaries[F]
    )

  def countFiles[F[_]]: Pipe[F, Entry, Metric] =
    _.fold(Set.empty[SourceId]) {
      case (sourceIds, entry) => sourceIds + entry.sourceId
    }.map(sourceIds => Metric.ProcessedFiles(sourceIds.size))

  def countMeasurements[F[_]]: Pipe[F, Entry, Metric] =
    _.fold(MeasurementsProcessed(0, 0)) {
      case (metric, Entry(_, _, Measurement.NaN)) =>
        MeasurementsProcessed.failed.modify(_ + 1)(metric)
      case (metric, _)                            =>
        MeasurementsProcessed.successful.modify(_ + 1)(metric)
    }

  def calculateSummaries[F[_]: Sync]: Pipe[F, Entry, Metric.ResultPerSensor] =
    _.fold(Map.empty[SensorId, IntermediateSummary]) {
      case (sensors, Entry(_, sensorId, Measurement.NaN))                =>
        sensors.updatedWith(sensorId) {
          case None => Some(IntermediateSummary(Measurement.NaN, Measurement.NaN, 0, 0))
          case x    => x
        }
      case (sensors, Entry(_, sensorId, measurement: Measurement.Value)) =>
        sensors.updatedWith(sensorId) {
          case Some(IntermediateSummary(min: Measurement.Value, max: Measurement.Value, sum, count)) =>
            Some(
              IntermediateSummary(
                Measurement.Value.ordering.min(min, measurement),
                Measurement.Value.ordering.max(max, measurement),
                sum + measurement.value.value,
                count + 1
              )
            )
          case _                                                                                     =>
            Some(
              IntermediateSummary(
                measurement,
                measurement,
                measurement.value.value,
                1
              )
            )
        }
    }.evalMap { sensorIdToIntermediateSummary =>
      sensorIdToIntermediateSummary
        .toList
        .traverse {
          case (sensorId, intermediateSummary) =>
            Sync[F].fromTry(intermediateSummary.toSensorSummary.map(sensorId -> _))
        }
        .map(Metric.ResultPerSensor.apply)
    }

  def sortSummaries[F[_]]: Pipe[F, Metric.ResultPerSensor, Metric.ResultPerSensor] =
    _.map { resultPerSensor =>
      resultPerSensor.copy(resultPerSensor.data.sortWith {
        case ((_, summaryA), (_, summaryB)) =>
          (summaryA.avg, summaryB.avg) match {
            case (valueA: Measurement.Value, valueB: Measurement.Value) =>
              implicitly[Ordering[Measurement.Value]].reverse.lt(valueA, valueB)
            case (Measurement.NaN, _)                                   => false
            case (_, Measurement.NaN)                                   => true
          }
      })
    }

  private case class IntermediateSummary(min: Measurement, max: Measurement, sum: BigInt, count: BigInt) {

    def toSensorSummary: Try[SensorSummary] = {
      val avgEither =
        if (count > 0)
          Measurement.ofInt((sum / count).toInt).leftMap(new IllegalStateException(_)).toTry
        else
          Success(Measurement.NaN)

      avgEither.map(avg => SensorSummary(min, avg, max))
    }

  }

}
