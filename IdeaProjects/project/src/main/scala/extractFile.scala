import org.apache.spark
import org.apache.spark.sql.SparkSession

import scala.io.{BufferedSource, Source}
import org.apache.spark.{SparkConf, SparkContext}

object extractFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("loadcsv")

    val sc = new SparkContext(conf) // Create Spark Context
    // Load local file data
    val spark : SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val emp_data = spark.read.csv("C:\\Users\\test\\parking_violations.csv")
    val df1 = emp_data.select("_c1", "_c5", "_c23", "_c24","_c39").show()

  }

}
