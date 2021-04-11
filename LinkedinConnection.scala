import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LinkedinConnection extends App{
  
  def parseLine(line: String) = {
    val field = line.split(",")
    val age = field(2).toInt
    val conn = field(3).toInt
    (age,conn)    
  }
    
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/Gofrugal/sparkpractice/src/friendsdata-201008-180523.csv")

  val mappedInput = input.map(parseLine) 
  
//  val mapFinal = mappedInput.map(x=> (x._1,(x._2,1))) (instead of map we can use mapValues)
  
  val mapFinal = mappedInput.mapValues(x=> (x,1))
  
  val finalCount = mapFinal.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  
//  val avgResult = finalCount.map(x => (x._1,x._2._1 / x._2._2))
  
  val avgResult = finalCount.mapValues(x => (x._1 / x._2))  
  
  val result =avgResult.collect.foreach(println)

  
   scala.io.StdIn.readLine()
 
}