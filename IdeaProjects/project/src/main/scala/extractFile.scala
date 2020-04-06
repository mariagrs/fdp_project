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
      .appName("loadcsv")
      .master("local[2]")
      .getOrCreate()


    val sourceDf = spark.read.format("csv").option("header", "true").load("C:\\Users\\test\\parking_violations.csv")
    val df1 = sourceDf.select("Plate ID", "Violation description", "House Number","Street Name").toDF()

    val randomRow = df1.sample(false,0.1).toDF()

    randomRow.show(1)

    val plate_id = randomRow.first().getString(0)
    val violation_description = randomRow.first().getString(1)
    val house_number = randomRow.first().getString(2)
    val street_name = randomRow.first().getString(3)

    if (plate_id.equals(null) || violation_description.equals(null)){
      print("alert")
    }
    else{
      print("Plate Id : "+ plate_id + " / Violation description : "+ violation_description + " / House number : "+ house_number + " / Street name : "+ street_name )
    }

    //df1.where(df1.col("Plate ID").isNull).show()
  }

}
