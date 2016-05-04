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

  // parse HTTP config
  val httpConfig = config.getConfig("http")
  val httpPort = httpConfig.getInt("port")
  val httpInterface = httpConfig.getString("interface")
  val httpTimeout = httpConfig.getInt("timeout")

  // parse Spark config
  val sparkConfig = config.getConfig("spark")
  val sparkJars = sparkConfig.getString("jars")
  val sparkMaster = sparkConfig.getString("master")
  val sparkMesosExecutorHome= sparkConfig.getString("mesos.executor-home")
  val sparkDriverCores = sparkConfig.getString("driver-cores")
  val sparkDriverMemory = sparkConfig.getString("driver-memory")
  val sparkMesosCoarse = sparkConfig.getString("mesos.coarse")
  val sparkMesosCoresMax : Option[String]= sparkMesosCoarse match{
    case "true" => Some(sparkConfig.getString("mesos.cores-max"))
    case "false" => None
  }
  val sparkMesosExecutorCores : Option[String]= sparkMesosCoarse match{
    case "false" => Some(sparkConfig.getString("mesos.executor-cores"))
    case "true" => None
  }
  val sparkExecutorMemory = sparkConfig.getString("executor-memory")
  val sparkCleanerTtl = sparkConfig.getString("cleaner-ttl")
  val sparkCheckpointDir = sparkConfig.getString("checkpoint-dir")
  val sparkEventLogDir = sparkConfig.getString("event-log-dir")
  val sparkEventLogEnabled = sparkConfig.getString("event-log-enabled")
  val sparkDefaultParallelism = sparkConfig.getString("default-parallelism")

  // parse cassandra config
  val cassandraConfig = config.getConfig("cassandra")
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

  // parse Akka settings
  val akkaConfig = config.getConfig("akka")
  val akkaRemotePort = akkaConfig.getString("remote.netty.tcp.port")
  val akkaSupervisorNbRetries = akkaConfig.getInt("supervision.max-nb-retries")

  // parse application settings
  val appConfig = config.getConfig("application")
  val hourZoneOffset = appConfig.getInt("hour-zone-offset")
  val topKLocationKeyspace = appConfig.getString("data-source.top-k-location.keyspace")
  val topKLocationTable = appConfig.getString("data-source.top-k-location.table")
  val usageKPIKeyspace = appConfig.getString("data-source.usage-kpi.keyspace")
  val usageKPITable = appConfig.getString("data-source.usage-kpi.table")
}

sealed trait ReactivePlatformEvent extends Serializable

case class TopKLocationQuery(k: Int, msisdn: String, cdrType: String, startTime: String, endTime: String) extends ReactivePlatformEvent

case class TopKLocationResponse(k: Int, msisdn: String, cdrType: String, startTime: String, endTime: String, locations : Seq[((Double,Double),Double)])
  extends ReactivePlatformEvent

case class UsageKPIQuery(msisdn: String, cdrType: String, startTime: String, endTime: String) extends ReactivePlatformEvent

case class UsageKPIResponse(msisdn: String,
                            cdrType: String,
                            startTime: String,
                            endTime: String,
                            nbEvents:Int,
                            nbEventsPerMinute: Double,
                            nbDistinctPeer: Long,
                            sumDuration:Int,
                            averageDuration: Int
                           ) extends ReactivePlatformEvent

object QueryApiJsonProtocol extends DefaultJsonProtocol{
  implicit val topKLocationQueryFormat = jsonFormat5(TopKLocationQuery)
  implicit val topKLocationResponseFormat = jsonFormat6(TopKLocationResponse)
  implicit val usageKPIqueryFormat = jsonFormat4(UsageKPIQuery)
  implicit val usageKPIResponseFormat = jsonFormat9(UsageKPIResponse)
}

object ApiUtils {
  def stringToEpoch(stringDate: String,offset:Int =0): Long = {
    LocalDateTime.parse(stringDate.replace(' ', 'T')).toInstant(ZoneOffset.ofHours(offset)).toEpochMilli()
  }

  def epochToString(epoch: Long): String = {
    Instant.ofEpochSecond(epoch).toString().replace('T', ' ')
  }
}