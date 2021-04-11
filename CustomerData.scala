import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object CustomerData extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   val sc= new SparkContext("local[*]","wordcount")
   
   val input = sc.textFile("D:/downloads/customerorders-201008-180523.csv")
   
   val mapped_input = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))

   val total_order = mapped_input.reduceByKey((x,y)=>x+y)   
   
   val sorted_order = total_order.sortBy(x => -x._2)
   
   val result = sorted_order.collect()
   
   result.foreach(println)
   //   scala.io.StdIn.readLine() 
  
}