package me.wojnowski.sensorstatistics.domain

import cats.Show
import cats.syntax.all._

sealed trait Metric extends Product with Serializable {
  def pretty: String
}

object Metric {

  case class ProcessedFiles(count: Int) extends Metric {
    def pretty = s"Num of processed files: $count"
  }

  case class MeasurementsProcessed(successful: Int, failed: Int) extends Metric {
    def pretty =
      s"""Num of processed measurements: $successful
         |Num of failed measurements: $failed""".stripMargin
  }

  case class ResultPerSensor(data: List[(SensorId, SensorSummary)]) extends Metric {

    override def pretty =
      s"""Sensors with highest avg humidity:
         |
         |sensor-id,min,avg,max
         |""".stripMargin +
        data
          .map {
            case (sensorId, sensorSummary) =>
              show"$sensorId,${sensorSummary.min},${sensorSummary.avg},${sensorSummary.max}"
          }
          .mkString("\n")

  }

  implicit val show: Show[Metric] = Show.show(_.pretty)
}
