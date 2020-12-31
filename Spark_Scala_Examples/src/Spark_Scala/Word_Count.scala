package Spark_Scala
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Word_Count {
  
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("F:/SPARK/RDD/THIRDPARTYLICENSEREADME.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()
    
    // Print the results
    wordCounts.foreach(println)
  }
  
}