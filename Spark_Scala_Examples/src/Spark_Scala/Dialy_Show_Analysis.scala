package Spark_Scala
import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object Dialy_Show_Analysis  
{
  
  def main(args : Array[String])
  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
    .builder()
    .appName("Daily_Show_Analysis")
    .master("local[*]")
    .getOrCreate()
    
    val lines = spark.sparkContext.textFile("D://PROJECT//Acadglid Project//Daily Show Spark Analysis//dialy_show_guests")
    
    val lines_1 = lines.map(_.split(","))
    
    val date_format = new java.text.SimpleDateFormat("MM/dd/yy")
    
    val lines_2 = lines_1.map((x => (x(1) ,date_format.parse(x(2)))))
    
    val lines_3 = lines_2.filter( x => {  if(x._2.after(date_format.parse("1/11/99"))  && x._2.before(date_format.parse("6/11/99"))) true else false  })
    
    val lines_4 = lines_3.map(x =>  (x._1 ,1)).reduceByKey(_+_).map(x => x.swap).sortByKey(false).take(5)
    
    lines_4.foreach(println)
  }
  
}