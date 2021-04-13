import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object bigdata_campaign extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/Gofrugal/sparkpractice/src/bigdatacampaigndata-201014-183159.csv")
  
  val map_input = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  
  val input_split = map_input.flatMapValues(x => x.split(" "))
  
  val final_map = input_split.map(x => (x._2.toLowerCase(),x._1))
  
  val final_arg = final_map.reduceByKey((x,y) => x+y)
  
  val final_sort = final_arg.sortBy(x => -x._2)
  
  final_sort.collect().foreach(println)
}