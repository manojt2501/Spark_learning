import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Timestamp

case class OrdersData (order_id: Int, order_date: Timestamp, order_customer_id: Option[Int],
    order_status: String)

object DF_example extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my application")
  sparkConf.set("spark.master", "local[2]")
  
    val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()  
  
    val orderDF = spark.read 
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path", "D:/Gofrugal/sparkpractice/src/orders-201019-002101.csv")
  .load()
  
  import spark.implicits._
  
  val ordersDs = orderDF.as[OrdersData]
  
  ordersDs.filter(x => x.order_id < 10).show()  
    
//  orderDF.filter("order_id < 10")  
    
    
  
  scala.io.StdIn.readLine()
  spark.stop() 
}