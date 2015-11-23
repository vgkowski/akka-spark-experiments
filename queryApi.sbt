name         := "queryApi"
organization := "com.reactivePlatform"
version      := "1.0"
scalaVersion := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" % "akka-actor_2.11" % "2.4.0",
    "com.typesafe.akka" % "akka-stream-experimental_2.11" % "1.0",
    "com.typesafe.akka" % "akka-http-core-experimental_2.11" % "1.0",
    "com.typesafe.akka" % "akka-http-experimental_2.11" % "1.0",
    "com.typesafe.akka" % "akka-http-spray-json-experimental_2.11" % "1.0",
    "com.datastax.spark" % "spark-cassandra-connector-embedded_2.11" % "1.5.0-M2",
    "com.typesafe.akka" % "akka-cluster_2.11" % "2.4.0",
    "io.spray" % "spray-json_2.11" % "1.3.2"
  )
}