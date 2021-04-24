import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object Spark_strAPI extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my application")
  sparkConf.set("spark.master", "local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()  
   
  val orderSchema_1 = StructType (List(
      StructField("order_id",IntegerType, false),
      StructField("order_date",TimestampType),
      StructField("order_customer_id",IntegerType, true),
      StructField("order_status",StringType)))      //explicit Schema programatic approach // true or false for nullable or not
      
      
   val orderSchema =   "order_id Int, order_date Timestamp, order_customer_id Int, order_status String"
  
  
  val orderDF = spark.read
  .option("header",true)
  .schema(orderSchema)
  .csv("D:/Gofrugal/sparkpractice/src/orders-201019-002101.csv")
 
// val groupedOrderDF = orderDF.repartition(4).where("order_customer_id > 10000")
//  .select("order_id","order_customer_id")
//  .groupBy("order_customer_id")
//  .count()
  
//  groupedOrderDF.show()
  
  orderDF.printSchema
  
  orderDF.show()
  
  scala.io.StdIn.readLine()
  
  spark.stop() 
}