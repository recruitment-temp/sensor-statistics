package me.wojnowski.sensorstatistics.domain

import cats.Show
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Interval
import eu.timepit.refined.refineV
import cats.syntax.all._

sealed trait Measurement extends Product with Serializable

object Measurement {
  case object NaN extends Measurement

  case class Value(value: Int Refined Interval.Closed[W.`0`.T, W.`100`.T]) extends Measurement

  object Value {
    implicit val ordering: Ordering[Value] = Ordering.by(_.value.value)
  }

  def parseString(s: String): Either[String, Measurement] =
    if (s === "NaN")
      Right(NaN)
    else
      s.toIntOption
        .toRight(s"Can't convert [$s] to integer")
        .flatMap(value => ofInt(value))

  def ofInt(value: Int): Either[String, Measurement.Value] = refineV[Interval.Closed[W.`0`.T, W.`100`.T]](value).map(Value.apply)

  implicit val show: Show[Measurement] = Show.show {
    case Measurement.Value(value) => value.value.toString
    case Measurement.NaN          => "NaN"
  }

}
