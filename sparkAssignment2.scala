import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import breeze.linalg.min

object sparkAssignment2 extends App{
  
  def parseLine(line:String)= {
    val fields = line.split(",") 
    val stationID = fields(0) 
    val entryType = fields(2)   
    val temperature = fields(3) 
    (stationID, entryType, temperature)
    }

  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc= new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/Gofrugal/sparkpractice/src/tempdata-201125-161348.csv")
  
  val parsedLines = input.map(parseLine)
  
  val minTemps = parsedLines.filter(x => x._2 == "TMIN")
  
  val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
  
//  stationTemps.foreach(println)
  
  val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))
  
  val results = minTempsByStation.collect()
  
  minTempsByStation.foreach(println)
  for (result <- results.sorted) {   
    val station = result._1       
    val temp = result._2      
    val formattedTemp = f"$temp%.2f F"      
println(s"$station minimum temperature: $formattedTemp")     }
  
}