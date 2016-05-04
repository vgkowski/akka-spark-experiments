name         := "queryApi"

organization := "com.orange.reactivePlatform"

version      := "1.1"

scalaVersion := "2.11.7"

unmanagedBase := baseDirectory.value / "custom_lib"

unmanagedJars in Compile := (baseDirectory.value ** "*.jar").classpath

enablePlugins(JavaAppPackaging)

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

val akkaVersion="2.4.4"
val kafkaVersion="0.9.0.1"
val akkaSeedVersion="0.1.5"
val sparkVersion="1.6.1"
val sparkCassandraVersion= "1.6.0-M2"

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" % "akka-actor_2.11" % akkaVersion,
    "com.typesafe.akka" % "akka-stream_2.11" % akkaVersion,
    "com.typesafe.akka" % "akka-http-core_2.11" % akkaVersion,
    "com.typesafe.akka" % "akka-http-experimental_2.11" % akkaVersion,
    "com.typesafe.akka" % "akka-http-spray-json-experimental_2.11" % akkaVersion,
    "com.typesafe.akka" % "akka-cluster_2.11" % akkaVersion,
    "com.typesafe.akka" % "akka-cluster-metrics_2.11" % akkaVersion,
    "org.apache.kafka" % "kafka_2.11" % kafkaVersion,
    //"com.datastax.spark" % "spark-cassandra-connector_2.11" % sparkCassandraVersion,
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
    "org.apache.spark" % "spark-core_2.11" % sparkVersion,
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
    "com.sclasen" % "akka-zk-cluster-seed_2.11" % akkaSeedVersion
  )
}

//mainClass in Compile := Some("com.reactivePlatform.QueryApiApp")

//scriptClasspath := Seq("src/main/resources") ++ scriptClasspath.value
