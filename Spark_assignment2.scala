import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Spark_assignment2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input_data = sc.textFile("D:/Gofrugal/sparkpractice/src/views*.csv")
  
  
}