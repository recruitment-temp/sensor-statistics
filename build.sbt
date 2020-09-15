name := "sensor-statistics"

version := "0.1"

scalaVersion := "2.13.3"

mainClass in Compile := Some("me.wojnowski.sensorstatistics.Main")

// TODO cleanup
// TODO compilation warnings plugin


// TODO extract versions here
val versions = new {
  val cats = new {
    val core = "2.2.0"
    val effect = "2.2.0"
  }
}

libraryDependencies += "org.typelevel" %% "cats-core" % versions.cats.core
libraryDependencies += "org.typelevel" %% "cats-effect" % versions.cats.effect
libraryDependencies += "org.gnieh" %% "fs2-data-csv" % "0.7.0"
libraryDependencies += "eu.timepit" %% "refined" % "0.9.15"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

libraryDependencies ++= Seq(
"io.chrisdavenport" %% "log4cats-slf4j"   % "1.1.1",  // Direct Slf4j Support - Recommended
)

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30"




libraryDependencies ++= List(
  "co.fs2" %% "fs2-core",
  "co.fs2" %% "fs2-io"
).map(_ % "2.2.2")