name         := "queryApi"

organization := "com.reactivePlatform"

version      := "1.0"

scalaVersion := "2.11.7"

enablePlugins(JavaAppPackaging)

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" % "akka-actor_2.11" % "2.4.0",
    "com.typesafe.akka" % "akka-stream-experimental_2.11" % "2.0-M1",
    "com.typesafe.akka" % "akka-http-core-experimental_2.11" % "2.0-M1",
    "com.typesafe.akka" % "akka-http-experimental_2.11" % "2.0-M1",
    "com.typesafe.akka" % "akka-http-spray-json-experimental_2.11" % "2.0-M1",
    "org.apache.kafka" % "kafka_2.11" % "0.8.2.2",
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.5.0-M2",
    "org.apache.spark" % "spark-streaming_2.11" % "1.5.2",
    "com.typesafe.akka" % "akka-cluster_2.11" % "2.4.0",
    "io.spray" % "spray-json_2.11" % "1.3.2",
    "com.sclasen" % "akka-zk-cluster-seed_2.11" % "0.1.2"
  )
}

//mainClass in Compile := Some("com.reactivePlatform.QueryApiApp")

//scriptClasspath := Seq("src/main/resources") ++ scriptClasspath.value
