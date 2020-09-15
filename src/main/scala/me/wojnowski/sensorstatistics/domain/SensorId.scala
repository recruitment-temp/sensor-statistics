package me.wojnowski.sensorstatistics.domain

import cats.Show
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.refineV

case class SensorId(value: String Refined NonEmpty)

object SensorId {
  def validate(s: String): Either[String, SensorId] = refineV[NonEmpty](s).map(SensorId.apply)

  implicit val show: Show[SensorId] = Show.show(_.value.value)
}
