package utils

import java.text.SimpleDateFormat

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector

import play.api.Play



object PagesViewed {

  def PagesViewedUtil {

    case class LogEvent(ip:String, timestamp:String, requestPage:String, responseCode:Int, responseSize:Int, userAgent:String)

    //method for parsing the logs passed in
    def parseLogEvent(event: String): LogEvent = {
      val LogPattern = """^([\d.]+) (\S+) (\S+) \[(.*)\] \"([^\s]+) (/[^\s]*) HTTP/[^\s]+\" (\d{3}) (\d+) \"([^\"]+)\" \"([^\"]+)\"$""".r
      val m = LogPattern.findAllIn(event)
      if (m.nonEmpty)
        new LogEvent(m.group(1), m.group(4), m.group(6), m.group(7).toInt, m.group(8).toInt, m.group(10))
      else
        null
    }

    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")

    val topics = "apache"
    val numThreads = 2
    val zkQuorum = "localhost:2181" // Zookeeper quorum (hostname:port,hostname:port)
    val clientGroup = "sparkFetcher"

    val driverPort = 8080
    val driverHost = "localhost"

    val conf = new SparkConf(false)
      .setAppName("PagesViewedUtil")
      .setMaster("local[4]")
      .set("spark.logConf", "true")
      .set("spark.driver.port", s"$driverPort")
      .set("spark.driver.host", s"$driverHost")
      .set("spark.akka.logLifecycleEvents", "true")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.executor.extraClassPath", "/home/plamb/Coding/Web_Analytics_POC/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.1.1-SNAPSHOT.jar")

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS ipAddresses")
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ipAddresses WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE ipAddresses.timeOnPage (IP text PRIMARY KEY, page map<text, bigint>)")
    }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("/home/plamb/Coding/Web_Analytics_POC/logtest/log-analyzer-streaming")

    // assign equal threads to process each kafka topic
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // create an input stream that pulls messages from a kafka broker
    val events = KafkaUtils.createStream(
      ssc, // StreamingContext object
      zkQuorum,
      clientGroup,
      topicMap // Map of (topic_name -> numPartitions) to consume, each partition is consumed in its own thread
      // StorageLevel.MEMORY_ONLY_SER_2 // Storage level to use for storing the received objects
    ).map(_._2)

    val logEvents = events
      .flatMap(_.split("\n")) // take each line of DStream
      .map(parseLogEvent) // parse that to log event

    val ipTimeStamp = logEvents.map[(String, (String, Long, Long))](event => {
      val time = dateFormat.parse(event.timestamp).getTime()

      (event.ip, (event.requestPage, time, time))
    })

    //This code groups all the pages hit + their timestamps by each IP address resulting in (IP, CollectionBuffer) then
    //applies a map function to the elements of the CollectionBuffer (mapValues) that groups them by the pages that were hit (groupBy), then
    //it filters out all the assets requested we don't care about (filterKeys) (images css files etc) and then it maps a function to
    // the values of the groupBy (i.e. the List of (page visited, timestamp timestamp) using foldLeft to reduce them to one session so as to
    //see the time spent on each page by each IP. TODO: break the mapValues/filterkeys calls into their own functions

    def updateValues( newValues: Seq[(String, Long, Long)],
                      currentValue: Option[Seq[(String, Long, Long)]]
                      ): Option[Seq[(String, Long, Long)]] = {

      Some(currentValue.getOrElse(Seq.empty) ++ newValues)

    }

    // In batch mode, use groupByKey, in streaming, updateStateByKey
    //this code still isnt getting sessions exactly right; it should compute the session when a log appears of the user getting
    //a new page, it should also account for 404's, however, i need real logs to work on that
    val grouped = ipTimeStamp.updateStateByKey(updateValues)
      .mapValues( a => a.groupBy(_._1)
      .filterKeys(a => {
      a.endsWith(".html") || a.endsWith(".php") || a.equals("/") //filter on requests we care about
    }).map(identity) //added for serialization issues

      .mapValues {
      case Nil => None;
      case (_, a, b) :: tail => //ignore the page String so we can return a (Long, Long)
        Some(x = tail.foldLeft((a, b)) {
          // Apply the foldLeft to each of the times, finding the min time and the max time for start/end
          case ((startTime, nextStartTime), (_, endTime, nextEndTime)) =>
            (startTime min nextStartTime, endTime max nextEndTime)
        })
          .map{case (firstTime, lastTime) => lastTime - firstTime};
      //this finds the total length of the session
    }.map(identity) //added for serialization issues
      ).map(identity) //added for serialization issues


    grouped.print()

    grouped.saveToCassandra("ipaddresses", "timeonpage", SomeColumns("ip", "page"))

    ssc.start()
    ssc.awaitTermination()


  }

}
