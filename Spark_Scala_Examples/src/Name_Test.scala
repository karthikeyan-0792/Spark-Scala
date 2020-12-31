import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j._

object Name_Test 
{
  
  def main(args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
    .builder()
    .appName("Name_Test")
    .master("local[*]")
    .getOrCreate()
    
    val lines = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").load("D:\\Examples\\sample.csv")
     
    case class names_test(Name : String , Age: String)
    
    import spark.implicits._
    
    val lines_1 = lines.toDF("Name","Age")
    
    lines_1.createOrReplaceTempView("list")
    
    val lines_2 : DataFrame = spark.sql("select Name , Age from list")
    
    val lines_3 = lines_2.withColumn("Adult" , when(col("Age") >= "18" , "Yes").otherwise("No"))
    .withColumn("Marital Status", when(col("Age") >= "18" || col("Name") === "Karthi" , "Married").otherwise("Not Married"))
    
    lines_3.write.format("csv").option("header", "true").save("D:\\Examples\\sample_op_1.csv")
    
    
    
  }
}