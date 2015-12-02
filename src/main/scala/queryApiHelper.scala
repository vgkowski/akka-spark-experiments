import java.io.File

import spray.json.DefaultJsonProtocol

import akka.japi.Util._

import com.typesafe.config.ConfigFactory

import com.datastax.spark.connector.cql.PasswordAuthConf
import com.datastax.driver.core.ConsistencyLevel

import java.time._
import java.time.format._

class QueryApiConfig(path :String) {
  val config= ConfigFactory.parseFile(new File(path))

  val httpConfig = config.getConfig("http")
  val httpPort = httpConfig.getInt("port")
  val httpInterface = httpConfig.getString("interface")
  val httpTimeout = httpConfig.getInt("timeout")

  val sparkConfig = config.getConfig("spark")
  val sparkMaster = sparkConfig.getString("master")
  val sparkCleanerTtl = sparkConfig.getInt("cleaner-ttl")
  val sparkStreamingBatchInterval = sparkConfig.getInt("streaming-batch-interval")
  val sparkCheckpointDir = sparkConfig.getString("checkpoint-dir")
  val sparkMesosExecutorHome= sparkConfig.getString("mesos.executor-home")

  val cassandraConfig = config.getConfig("cassandra")
  val cassandraLocationKeyspace = cassandraConfig.getString("keyspace")
  val cassandraLocationTable = cassandraConfig.getString("table")
  val cassandraHosts = cassandraConfig.getString("cassandraHosts")
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

  val cassandraWriteBatchSizeRows: Option[Int] = {
    val NumberPattern = "([0-9]+)".r
    cassandraConfig.getString("write.batch-size-rows") match {
      case "auto"           => None
      case NumberPattern(x) => Some(x.toInt)
      case other =>
        throw new IllegalArgumentException(
          s"Invalid value for 'cassandra.output.batch.size.rows': $other. Number or 'auto' expected")
    }
  }
  val cassandraWriteConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("write.consistency-level"))
  val cassandraDefaultMeasuredInsertsCount = cassandraConfig.getInt("write.default-measured-inserts-count")

  val akkaConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 0")
}

sealed trait ReactivePlatformEvent extends Serializable

case class TopKLocationQuery(k: Int, msisdn: String, cdrType: String, startTime: String, endTime: String) extends ReactivePlatformEvent

case class TopKLocationResponse(k: Int, msisdn: String, cdrType: String, startTime: String, endTime: String, locations : Seq[(Double,Double)])
  extends ReactivePlatformEvent

object QueryApiJsonProtocol extends DefaultJsonProtocol{
  implicit val topKLocationQueryFormat = jsonFormat5(TopKLocationQuery)
  implicit val topKLocationResponseFormat = jsonFormat6(TopKLocationResponse)
}

object ApiUtils {
  def stringToEpoch(stringDate: String): Long = {
    LocalDateTime.parse(stringDate.replace(' ', 'T')).toInstant(ZoneOffset.ofHours(0)).toEpochMilli()
  }

  def epochToString(epoch: Long): String = {
    Instant.ofEpochSecond(epoch).toString().replace('T', ' ')
  }
}