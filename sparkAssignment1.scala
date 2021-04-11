import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object sparkAssignment1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/Gofrugal/sparkpractice/src/assignment1.dataset1")
  
  val rdd2 = input.map(line => {
    val fields = line.split(",")    
    if (fields(1).toInt > 18)  
      (fields(0),fields(1),fields(2),"Y") 
    else 
      (fields(0),fields(1),fields(2),"N")
      })
  
  rdd2.collect.foreach(println)
  
}