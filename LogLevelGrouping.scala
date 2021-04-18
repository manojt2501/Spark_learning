import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LogLevelGrouping extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
    
  val baseRdd = sc.textFile("D:/downloads/bigLogtxt-201014-183159/bigLog.txt")
  
 val mappedRdd =  baseRdd.map(x =>{
    val fields= x.split(":")
//    (fields(0), fields(1))
    (fields(0),1)
  }
)
//mappedRdd.groupByKey.collect().foreach(x =>  println(x._1, x._2.size))
mappedRdd.reduceByKey(_+_).collect.foreach(println)
scala.io.StdIn.readLine()
}