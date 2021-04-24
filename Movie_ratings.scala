import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Movie_ratings extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   val sc= new SparkContext("local[*]","wordcount")
   
   val input = sc.textFile("D:/Gofrugal/sparkpractice/src/ratings-201019-002101.dat")
   
   val map_RDD = input.map(x => {val fields= x.split("::")
     (fields(1),fields(2))})
     
   val new_RDD = map_RDD.mapValues(x => (x.toFloat,1.0))
   
   val reduce_RDD = new_RDD.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
   
   val top_RDD = reduce_RDD.filter(x =>x._2._2 >10)
   
   val div_RDD = top_RDD.mapValues( x => (x._1.toDouble/x._2))   
   
   val fil_RDD = div_RDD.filter(x => x._2 > 4)
   
   val movie_data = sc.textFile("D:/Gofrugal/sparkpractice/src/movies-201019-002101.dat")
   
   val mv_RDD = movie_data.map( x => { val fields= x.split("::")
     (fields(0),fields(1))
   })
   
   val joinedRDD = fil_RDD.join (mv_RDD)
   
   val finalRDD = joinedRDD.map(x => x._2._2)
   
   finalRDD.collect().foreach(println)
   
   scala.io.StdIn.readLine()
}