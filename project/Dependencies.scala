import sbt._

object Dependencies {
  lazy val kafkaConnect = "org.apache.kafka" % "connect-api" % "1.1.0"
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}
