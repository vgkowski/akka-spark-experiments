import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.event.Logging
import akka.pattern.pipe

import org.apache.spark.streaming.StreamingContext

import com.datastax.spark.connector.streaming._

class SparkQueryActor (config: QueryApiConfig,ssc: StreamingContext) extends Actor with ActorLogging{

  import context.dispatcher

  def receive = {
    case query:TopKLocationQuery => topK(query,sender)
  }

  def topK(query : TopKLocationQuery, requester: ActorRef): Unit ={

    val toTopK= (aggregate: Seq[(Double,Double)]) => TopKLocationResponse(query.k, query.msisdn,query.cdrType,
      query.startTime,query.endTime,ssc.sparkContext.parallelize(aggregate).map(p => ((p._1,p._2),1))
        .reduceByKey((v1,v2) => v1+v2)
        .map(_.swap)
        .top(query.k).map(a=>(a._2._1,a._2._2)).toSeq)

    ssc.cassandraTable[(Double,Double)](config.cassandraLocationKeyspace, config.cassandraLocationTable)
      .select("latitude","longitude")
      .where("msisdn=? AND cdr_type=? AND date_time > ? AND date_time < ?", query.msisdn, query.cdrType,
        ApiUtils.stringToEpoch(query.startTime), ApiUtils.stringToEpoch(query.endTime))
      .collectAsync().map(toTopK) pipeTo requester
  }
}