import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.Logging
import akka.pattern.pipe
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
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

    val toTopK= (aggregate: Seq[(Double,Double)]) => {
      val count= aggregate.size.toDouble
      TopKLocationResponse(query.k, query.msisdn,query.cdrType,query.startTime,query.endTime,
        sc.parallelize(aggregate).map(p => ((p._1,p._2),(1,count)))
          .reduceByKey((v1,v2) => (v1._1+v2._1,v1._2))
          .map{ case(geo,(s,c))=> ((s.toDouble/c*100.0),geo)}
          .top(query.k).map(_.swap).toSeq)
    }

    sc.cassandraTable[(Double,Double)](config.topKLocationKeyspace, config.topKLocationTable)
      .select("latitude","longitude")
      .where("msisdn=? AND cdr_type=? AND date_time > ? AND date_time < ?", query.msisdn, query.cdrType,
        ApiUtils.stringToEpoch(query.startTime,config.hourZoneOffset), ApiUtils.stringToEpoch(query.endTime,config.hourZoneOffset))
      .collectAsync().map(toTopK) pipeTo requester
  }

  def usageKPI(query:UsageKPIQuery,requester:ActorRef): Unit={

    val end= ApiUtils.stringToEpoch(query.endTime,config.hourZoneOffset)
    val start = ApiUtils.stringToEpoch(query.startTime,config.hourZoneOffset)
    val toUsageKpi = (rdd: RDD[(Int,String)]) => {
      val distinctPeer = rdd.map(_._2).distinct.countAsync()
      val stats= rdd.map(v=>(v._1,1)).reduce((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
      Future(UsageKPIResponse(query.msisdn, query.cdrType, query.startTime, query.endTime,
        stats._2, stats._2/((end-start)/60000),distinctPeer.get(),stats._1,stats._1/stats._2))
    }

    toUsageKpi(sc.cassandraTable[(Int,String)](config.usageKPIKeyspace, config.usageKPITable)
      .select("duration","peer_msisdn")
      .where("msisdn=? AND cdr_type=? AND date_time > ? AND date_time < ?", query.msisdn, query.cdrType,start,end)) pipeTo requester

  }
}