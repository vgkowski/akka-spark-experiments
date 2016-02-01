import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.event.Logging
import akka.pattern.pipe

import org.apache.spark.SparkContext

import com.datastax.spark.connector._

class SparkQueryActor (config: QueryApiConfig,sc: SparkContext) extends Actor with ActorLogging{

  import context.dispatcher

  def receive = {
    case query:TopKLocationQuery => topK(query,sender)
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

    sc.cassandraTable[(Double,Double)](config.cassandraLocationKeyspace, config.cassandraLocationTable)
      .select("latitude","longitude")
      .where("msisdn=? AND cdr_type=? AND date_time > ? AND date_time < ?", query.msisdn, query.cdrType,
        ApiUtils.stringToEpoch(query.startTime), ApiUtils.stringToEpoch(query.endTime))
      .collectAsync().map(toTopK) pipeTo requester
  }
}