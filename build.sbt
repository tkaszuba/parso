name := "parso"
version := "0.1.0"
organization := "com.github.tkaszuba"

scalaVersion := "2.13.0-M5"

scalacOptions ++= Seq("-target:jvm-1.7" )
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "2.7"
)

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

//include provided dependencies in sbt run task
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

//only one living spark-context is allowed
parallelExecution in Test := false

//skip test during assembly
test in assembly := {}
