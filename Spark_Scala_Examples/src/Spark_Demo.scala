import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object Spark_Demo 
{

  def main(args:Array[String])
  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
    .builder()
    .appName("Demo")
    .master("local[*]")
    .getOrCreate()
    
    val lines = spark.sparkContext.textFile("D:\\PROJECT\\Spark Project Data Files\\Azar Data Files\\ml-1m\\ratings.dat")
    
    val lines_1 = lines.map(_.split("::"))
    
    val lines_2 = lines_1.map(x => (x(0) , x(1))).map(_.swap)

    val lines_3 = lines_2.mapValues(x => (x,1))
    
    val lines_4 = lines_3.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    
    /*lines_4.collect().foreach(println)*/
    
   /* val lines_5 = lines_1.filter(x => { if((x(1) == "1193"))true else false}).map(x => (x(1) ,x(2)))*/
    
    val lines_5 = lines_1.map(x => (x(1) , x(2)))
    
    val lines_6 = lines_5.collect().foreach(println)
    

    
  }
  
}