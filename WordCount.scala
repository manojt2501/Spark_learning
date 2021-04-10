import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   val sc= new SparkContext("local[*]","wordcount")
   
   val input = sc.textFile("C:/Users/manoj/OneDrive/Desktop/spark.txt")
   
   val words = input.flatMap(_.split(" "))
   
   val wordslower= words.map(_.toLowerCase())
   
   val wordMap = wordslower.map(x => (x,1))
   
   val wordCount = wordMap.reduceByKey((x,y) => x+y)
   
   val finalCount = wordCount.map( x=> (x._2,x._1)).sortByKey(false).map( x=> (x._2,x._1)) 
   
//   finalCount.collect.foreach(println)
   val results = finalCount.collect
   
   for (result <- results){
   val word = result._1
   val count = result._2
   println(s"$word: $count")
   }
   
   scala.io.StdIn.readLine()
}
