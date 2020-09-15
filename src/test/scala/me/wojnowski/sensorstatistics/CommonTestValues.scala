package me.wojnowski.sensorstatistics

import cats.scalatest.EitherValues
import me.wojnowski.sensorstatistics.domain.Entry
import me.wojnowski.sensorstatistics.domain.Measurement
import me.wojnowski.sensorstatistics.domain.SensorId
import me.wojnowski.sensorstatistics.domain.SourceId
import cats.syntax.all._
import me.wojnowski.sensorstatistics.domain.SensorSummary
import eu.timepit.refined.auto._

trait CommonTestValues extends EitherValues {
  val SourceId1 = SourceId("1")
  val SourceId2 = SourceId("2")

  val SensorId1 = SensorId("s1")
  val SensorId2 = SensorId("s2")
  val SensorId3 = SensorId("s3")
  val SensorId4 = SensorId("s4")

  def entry(sourceId: String, sensorId: String, measurement: Measurement): Entry =
    (
      SourceId.validate(sourceId),
      SensorId.validate(sensorId)
    ).mapN(Entry(_, _, measurement)).value

  def entry(sourceId: String, sensorId: String, measurement: Int): Entry =
    entry(sourceId, sensorId, Measurement.ofInt(measurement).value)

  def summary(min: Int, avg: Int, max: Int): SensorSummary =
    (
      Measurement.ofInt(min),
      Measurement.ofInt(avg),
      Measurement.ofInt(max)
    ).mapN(SensorSummary.apply).value

}
