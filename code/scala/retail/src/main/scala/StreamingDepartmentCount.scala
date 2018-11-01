import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StreamingDepartmentCount {
  def main(args: Array[String])={
    val executionMode=args(0)
    val host=args(1)
    val port=args(2).toInt
    val outputPath = args(3)
    val conf = new SparkConf().setAppName("Streaming Department Count").setMaster(executionMode)
    val ssc = new StreamingContext(conf, Seconds(30))

    val messages = ssc.socketTextStream(host, port)
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
