import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Spark_assignment extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input_data = sc.textFile("D:/Gofrugal/sparkpractice/src/chapters-201108-004545.csv")
  
  val titlesDataRDD = sc.textFile("D:/Gofrugal/sparkpractice/src/titles-201108-004545.csv").map( x => (x.split(",")(0).toInt, x.split(",")(1)))
  
  val ChapterRdd = input_data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
  
  val map_data = input_data.map(x => (x.split(",")(1).toInt,1))
  
  val agg_data = map_data.reduceByKey((x,y) => x+y)
  
//  agg_data.collect.sorted.foreach(println)
  
  val view_data = sc.textFile("D:/Gofrugal/sparkpractice/src/views*.csv")
  
  val view_mapdata = view_data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
  
  val viewDataDistinctRDD = view_mapdata.distinct()
  
  val flippedviewDataRDD = viewDataDistinctRDD.map(x => (x._2,x._1))
  
  val joinedRDD = flippedviewDataRDD.join (ChapterRdd)
  
  val pairRDD = joinedRDD.map  ( x => ((x._2._1, x._2._2),1))
  
  val finalAgg_data = pairRDD.reduceByKey((x,y) => x+y)
  
  val courseViewsCountRDD = finalAgg_data.map( x => (x._1._2,x._2))
  
  val newJoinedRDD   =  courseViewsCountRDD.join(agg_data)
  
  val CourseCompletionpercentRDD = newJoinedRDD.mapValues( x => (x._1.toDouble/x._2))
  
  val scoresRDD =  CourseCompletionpercentRDD.mapValues (x => {
    if(x >= 0.9)  
      10       
    else if(x >=  0.5 &&  x < 0.9) 
    4
    else if(x >=  0.25 &&  x < 0.5) 
    2
    else 
     0 }
  
  )  
  
  val totalScorePerCourseRDD =   scoresRDD.reduceByKey((V1,V2) => V1 + V2)
  
  val title_score_joinedRDD =  totalScorePerCourseRDD.join(titlesDataRDD).map  ( x => (x._2._1, x._2._2))


  title_score_joinedRDD.collect.foreach(println)
}