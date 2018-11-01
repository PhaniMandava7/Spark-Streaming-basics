import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object KafkaStreamingDepartmentCount {
  def main(args: Array[String])={
    val executionMode=args(0)
    val outputPath = args(1)
	val kafkaBrokerList = args(2)
    val conf = new SparkConf().setAppName("Kafka Streaming Department Count").setMaster(executionMode)
    val ssc = new StreamingContext(conf, Seconds(30))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)
    val topicSet = Set("flumeKafka")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    val messages = stream.map(s => s._2)
    val filteredMessages = messages.filter(msg => {
    val endPoint = msg.split(" ")(6)
    endPoint.split("/")(1) == "department"
    })
    val mapDepartments = filteredMessages.
    map(msg => {
    val endPoint=msg.split(" ")(6)
    (endPoint.split("/")(2),1)
    })
    val departmentTraffic = mapDepartments.reduceByKey(_+_)
    println(departmentTraffic)

    departmentTraffic.saveAsTextFiles(outputPath)
    ssc.start()
    ssc.awaitTermination()
  }
}

