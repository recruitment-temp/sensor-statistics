package me.wojnowski.sensorstatistics

import cats.effect.Concurrent
import fs2.Pipe
import me.wojnowski.sensorstatistics.DataProcessors.IntermediateSummary
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.Metric
import me.wojnowski.sensorstatistics.domain.Metric.MeasurementsProcessed
import me.wojnowski.sensorstatistics.domain.SensorId
import me.wojnowski.sensorstatistics.domain.SensorSummary
import me.wojnowski.sensorstatistics.domain.SourceId
import cats.syntax.all._

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
        metric.copy(failed = metric.failed + 1)
      case (metric, _)                            =>
        metric.copy(successful = metric.successful + 1) // TODO lenses
    }

  def calculateSummaries[F[_]]: Pipe[F, Entry, Metric.ResultPerSensor] =
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
    }.map { sensorIdToIntermediateSummary =>
      Metric.ResultPerSensor(
        sensorIdToIntermediateSummary
          .toList
          .map {
            case (sensorId, intermediateSummary) =>
              sensorId -> SensorSummary(intermediateSummary.min, intermediateSummary.avg, intermediateSummary.max)
          }
      )
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

    def avg: Measurement =
      if (count > 0)
        Measurement.ofInt((sum / count).toInt).getOrElse(Measurement.NaN) // TODO this is programmers error
      else
        Measurement.NaN

  }

}
