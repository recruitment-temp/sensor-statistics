package me.wojnowski.sensorstatistics.domain

case class SensorSummary(min: Measurement, avg: Measurement, max: Measurement)

object SensorSummary {
  implicit val ordering: Ordering[SensorSummary] = Ordering.fromLessThan { (a, b) =>
    (a.avg, b.avg) match {
      case (a: Measurement.Value, b: Measurement.Value) => implicitly[Ordering[Measurement.Value]].lt(a, b)
      case (Measurement.NaN, b) => false
      case (a, Measurement.NaN) => true
    }
  }
}