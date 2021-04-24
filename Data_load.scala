import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Data_load extends App{
     val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my application")
  sparkConf.set("spark.master", "local[2]")
  
      val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()  
  
//      val playerDF = spark.read 
//  .format("json")
//  .option("path", "D:/Gofrugal/sparkpractice/src/players.json")
//  .option("mode","FAILFAST")
//  .load()
  
   val usersDF = spark.read 
  .format("parquet")
  .option("path", "D:/Gofrugal/sparkpractice/src/users-201019-002101.parquet")
  .load()
   
  
  usersDF.printSchema
  
  usersDF.show(false)
  
  spark.stop() 
  
}