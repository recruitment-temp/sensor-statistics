name := "sensor-statistics"

version := "0.1"

scalaVersion := "2.13.3"
scalacOptions in Global += "-Ymacro-annotations"

mainClass in Compile := Some("me.wojnowski.sensorstatistics.Main")

val versions = new {

  val cats = new {
    val core = "2.2.0"
    val effect = "2.2.0"
  }

  val monocle = "2.0.3"

  val fs2 = new {
    val core = "2.2.2"
    val dataCsv = "0.7.0"
  }

  val refined = "0.9.15"
  val scalatest = "3.1.1"

  val log4cats = "1.1.1"
  val slf4jSimple = "1.7.30"
}

val cats = Seq(
  "org.typelevel" %% "cats-core" % versions.cats.core,
  "org.typelevel" %% "cats-effect" % versions.cats.effect
)

val fs2 = Seq(
  "co.fs2" %% "fs2-core" % versions.fs2.core,
  "co.fs2" %% "fs2-io" % versions.fs2.core,
  "org.gnieh" %% "fs2-data-csv" % versions.fs2.dataCsv
)

val monocle = Seq(
  "com.github.julien-truffaut" %% "monocle-core" % versions.monocle,
  "com.github.julien-truffaut" %% "monocle-macro" % versions.monocle
)

val refined = Seq("eu.timepit" %% "refined" % versions.refined)

val scalatest = Seq("org.scalatest" %% "scalatest" % versions.scalatest % "test")

val logging = Seq(
  "io.chrisdavenport" %% "log4cats-slf4j" % versions.log4cats,
  "org.slf4j" % "slf4j-simple" % versions.slf4jSimple
)


libraryDependencies ++= cats ++ fs2 ++ monocle ++ refined ++ scalatest ++ logging
