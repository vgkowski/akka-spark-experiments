import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.Logging
import akka.pattern.pipe
import org.apache.spark.SparkContext
import com.datastax.spark.connector.{rdd, _}
import org.apache.spark.rdd.RDD

import scala.concurrent.Future

object SparkQueryActor{
  def props(config:QueryApiConfig,sc:SparkContext) : Props= Props(new SparkQueryActor(config,sc))
}

class SparkQueryActor (config: QueryApiConfig,sc: SparkContext) extends Actor with ActorLogging{

  import context.dispatcher

  def receive = {
    case query:TopKLocationQuery => topK(query,sender)
    case query:UsageKPIQuery => usageKPI(query,sender)
  }

  def topK(query : TopKLocationQuery, requester: ActorRef): Unit ={

    val end= ApiUtils.stringToEpoch(query.endTime,config.hourZoneOffset)
    val start = ApiUtils.stringToEpoch(query.startTime,config.hourZoneOffset)

    val toTopK= (aggregate: Seq[(Double,Double)]) => {
      val count= aggregate.size.toDouble
      TopKLocationResponse(query.k, query.msisdn,query.cdrType,query.startTime,query.endTime,
        sc.parallelize(aggregate).map(p => ((p._1,p._2),(1,count)))
          .reduceByKey((v1,v2) => (v1._1+v2._1,v1._2))
          .map{ case(geo,(s,c))=> (s.toDouble / c * 100.0,geo)}
          .top(query.k).map(_.swap).toSeq)
    }

    sc.cassandraTable[(Double,Double)](config.topKLocationKeyspace, config.topKLocationTable)
      .select("latitude","longitude")
      .where("msisdn=?", query.msisdn)
      .where("cdr_type=?",query.cdrType )
      .where("date_time>?", start)
      .where("date_time<?", end)
      .collectAsync().map(toTopK) pipeTo requester
  }

  def usageKPI(query:UsageKPIQuery,requester:ActorRef): Unit={

    // convert date from string to epoch millis
    val end= ApiUtils.stringToEpoch(query.endTime,config.hourZoneOffset)
    val start = ApiUtils.stringToEpoch(query.startTime,config.hourZoneOffset)

    log.info("cassandra query parameters {} {} {} {}", query.msisdn,query.cdrType,query.startTime,query.endTime)
    // load raw data from Cassandra in async mode (a Future)
    val cassandraLoad= Future{
      sc.cassandraTable[(Int,String)](config.usageKPIKeyspace, config.usageKPITable)
        .select("duration","peer_msisdn")
        .where("msisdn=? AND cdr_type=? AND date_time > ? AND date_time < ?",query.msisdn, query.cdrType,start,end)
    }

    // count the distinct peer in async (API) when the first async load is completed
    val distinctPeer = cassandraLoad flatMap { rdd =>
      rdd.map(_._2).distinct.countAsync()
    }

    // process others KPIs in async mode (a Future) when the first async load is completed
    val otherStats = cassandraLoad flatMap { rdd =>
      Future {
        // process KPIs
        rdd.map(v => (v._1, 1))
          .reduce((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      } map { stats =>
        // return a tuple of (count, count per minute, sum of duration, average duration)
        (stats._2, stats._2/((end-start)/60000),stats._1,stats._1/stats._2)
      }
    }

    // combine the 2 Futures values when completed into a Future response
    // return a UsageKPIResponse
    for {
      distinct <- distinctPeer
      others <- otherStats
    } yield requester ! UsageKPIResponse(query.msisdn, query.cdrType, query.startTime, query.endTime,
        others._1, others._2,distinct,others._3,others._4)
  }
}