import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object streamingWordCount{
  def main(args: Array[String]) = {
    val executionMode = args(0)
    val host = args(1)
    val port = args(2).toInt
    val conf = new SparkConf().setAppName("streaming-word-count").setMaster(executionMode)
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream(host,port)
    val words = lines.flatMap(line => line.split(" "))
    val tuples = words.map(word => (word,1))
    val wordCounts = tuples.reduceByKey(_+_)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}




















