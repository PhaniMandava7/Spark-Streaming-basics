import scala.io.Source
object orderRevenue{
  def main(args: Array[String]) ={
    val source= args(0)
    val order_items = Source.fromFile(source).getLines
    val orderRevenue = order_items.filter(o => o.split(",")(1).toInt==2).
    map(o => o.split(",")(4).toFloat).
    reduce(_+_)
    println(orderRevenue)
  }
}
