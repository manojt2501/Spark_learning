import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object bigdata_campaign extends App{
  
  def loadBoardingWords():Set[String] = {
  var boaringWords:Set[String] = Set()
  val lines = Source.fromFile("D:/Gofrugal/sparkpractice/src/boring_words.txt").getLines()    
  for (line <- lines){
    boaringWords += line
  }
    boaringWords
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  var name_set = sc.broadcast(loadBoardingWords)
  
  val input = sc.textFile("D:/Gofrugal/sparkpractice/src/bigdatacampaigndata-201014-183159.csv")
  
  val map_input = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  
  val input_split = map_input.flatMapValues(x => x.split(" "))
  
  val final_map = input_split.map(x => (x._2.toLowerCase(),x._1))
  
  val rdd_filter = final_map.filter(x => !name_set.value(x._1))
  
  val final_arg = rdd_filter.reduceByKey((x,y) => x+y)
  
  val final_sort = final_arg.sortBy(x => -x._2)
  
  final_sort.collect().foreach(println)
}