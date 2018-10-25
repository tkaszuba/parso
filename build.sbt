name := "parso"
version := "0.1.0"
organization := "com.kaszub"

scalaVersion := "2.13.0-M4"

scalacOptions ++= Seq("-target:jvm-1.8" )
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.6-SNAP2" % Test,
  "org.typelevel" %% "cats-core" % "1.4.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.slf4j" % "slf4j-simple" % "1.7.25" % Test
)

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

//include provided dependencies in sbt run task
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

//only one living spark-context is allowed
Test / parallelExecution := false

//skip test during assembly
//test in assembly := {}
