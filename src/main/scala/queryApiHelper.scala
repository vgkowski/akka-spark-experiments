import java.io.File

import com.typesafe.config.ConfigFactory

import com.datastax.spark.connector.cql.{AuthConf, PasswordAuthConf}
import com.datastax.driver.core.ConsistencyLevel

class ingestionApiConfig(path :String) {
  val config= ConfigFactory.parseFile(new File(path))

  val httpConfig = config.getConfig("http")
  val httpPort = httpConfig.getInt("port")
  val httpInterface = httpConfig.getString("interface")

  val sparkConfig = config.getConfig("spark")
  val sparkMaster = sparkConfig.getString("master")
  val sparkCleanerTtl = sparkConfig.getInt("cleaner-ttl")
  val sparkStreamingBatchInterval = sparkConfig.getInt("streaming-batch-interval")
  val sparkCheckpointDir = sparkConfig.getString("checkpoint-dir")

  val cassandraConfig = config.getConfig("cassandra")
  val cassandraHosts = cassandraConfig.getString("hosts")
  val cassandraAuthUsername = cassandraConfig.getString("auth-username")
  val cassandraAuthPassword = cassandraConfig.getString("auth-password")
  val cassandraAuth = PasswordAuthConf(cassandraAuthUsername,cassandraAuthPassword)
  val cassandraKeepAlive = cassandraConfig.getInt("keep-alive")
  val cassandraRetryCount = cassandraConfig.getInt("retry-count")
  val cassandraReconnectDelayMin = cassandraConfig.getInt("connection-reconnect-delay-min")
  val cassandraReconnectDelayMax = cassandraConfig.getInt("connection-reconnect-delay-max")
  val cassandraReadPageRowSize = cassandraConfig.getString("read.page-row-size")
  val cassandraReadConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("read.consistency-level"))
  val cassandraReadSplitSize = cassandraConfig.getInt("read.split-size")
  val cassandraWriteParallelismLevel = cassandraConfig.getInt("write.parallelism-level")
  val cassandraWriteBatchSizeBytes = cassandraConfig.getInt("write.batch-size-bytes")
  val cassandraWriteBatchSizeRows = cassandraConfig.getInt("write.batch-size-rows")
  val cassandraWriteConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getInt("write.consistency-level"))
  val cassandraDefaultMeasuredInsertsCount = cassandraConfig.getInt("default-measured-inserts-count")

  val akkaConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 0")
}