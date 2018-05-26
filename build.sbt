import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.engitano",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Kafka Connect Struct Builder",
    libraryDependencies ++= Seq(kafkaConnect, shapeless, scalaTest % Test)
  )
