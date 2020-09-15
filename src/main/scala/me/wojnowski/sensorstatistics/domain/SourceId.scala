package me.wojnowski.sensorstatistics.domain

import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

case class SourceId(value: String Refined NonEmpty)

object SourceId {
  def validate(s: String): Either[String, SourceId] = refineV[NonEmpty](s).map(SourceId.apply)
}
