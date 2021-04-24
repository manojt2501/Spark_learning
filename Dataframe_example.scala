import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Dataframe_example extends App{
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my application")
  sparkConf.set("spark.master", "local[2]")
  
  
  val Spark = SparkSession.builder()
//  .appName("my application 1")
//  .master("local[2]") (or)
  .config(sparkConf)
  .getOrCreate()  
  
  
  Spark.stop()
}