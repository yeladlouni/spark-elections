import org.apache.spark.sql.SparkSession

object Election extends App {
  val spark = SparkSession
    .builder()
    .appName("ElectionAnalysis")
    .config("spark.master", "local")
    .getOrCreate()

  val df_biden = spark.read
    .option("header", "true")
    .load("biden.csv")

  val df_trump = spark.read
    .option("header", "true")
    .load("trump.csv")

}