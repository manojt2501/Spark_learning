import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object RatingCalculator extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/Gofrugal/sparkpractice/src/moviedata-201008-180523.data")
  
  val mapped_input = input.map(x => x.split("\t")(2))
  
  val finalCount = mapped_input.countByValue()
  
// below is the alternative way to count values
//  val rating_input = mapped_input.map(x => (x,1))
//  
//  val ratingCount = rating_input.reduceByKey((x,y) => x+y)
//  
//  val finalCount = ratingCount.sortBy(x => x._1).collect()
  
  finalCount.foreach(println)
  
  scala.io.StdIn.readLine()
  
}