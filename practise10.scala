import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object practise10 extends App{
  
//  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val myList= List("warn: asdfadfa","error: adsfads","warn: asdfasdf","warn: asdafdcd","error: lasdcoiasd","warn: aodifjoaidjfo")
  
  val list_Rdd = sc.parallelize(myList)
  
  val new_Rdd= list_Rdd.map(x => 
    { val columns = x.split(":")
      val level = columns(0)
      (level,1)     
    })
    val result = new_Rdd.reduceByKey((x,y) => x+y)
    
    result.collect().foreach(println)
    
    scala.io.StdIn.readLine()
}