import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, repeat}

object Election extends App {
  val spark = SparkSession
    .builder()
    .appName("ElectionAnalysis")
    .config("spark.master", "local")
    .getOrCreate()

  var df_biden = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("readmode", "permissive")
    .load("C:\\Users\\yelad\\IdeaProjects\\spark-elections\\data\\hashtag_joebiden.csv")

  var df_trump = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:\\Users\\yelad\\IdeaProjects\\spark-elections\\data\\hashtag_donaldtrump.csv")

  df_biden.printSchema()
  df_trump.printSchema()



  // Afin de faire la fusion des deux dataframe et les distinguer par la suite, on ajoute la colonne candidate

  df_biden = df_biden.withColumn("candidate", lit("biden"))
    .select("created_at", "tweet_id", "likes", "retweet_count", "city", "country", "state", "candidate")
  df_trump = df_trump.withColumn("candidate", lit("trump"))
    .select("created_at", "tweet_id", "likes", "retweet_count", "city", "country", "state", "candidate")

  df_biden.show()
  df_trump.show()

  // Jointure des deux dataframe

  var df = df_biden.union(df_trump)

  df.show()

  println(df.count())

  // Cleansing

  df = df.dropDuplicates("tweet_id")

  println(df.count())

  df.na.drop("all")

  // Wrangling : TODO

  df.select("candidate", "likes")

  // Stocker le résultat final
  // Parquet ==> Twitter est un format de sérialisation qui contient à l'intérieur le schéma des données
  // Orc ==> Facebook
  // ProtoBuff ==> Google
  // Avro
  df.write
    .parquet("C:\\Users\\yelad\\IdeaProjects\\spark-elections\\output")


  // Souvent en machine learning, on fait un split des données d'une manière aléatoire
  // On applique l'algorithme sur la partie train pour l'apprentissage, dev pour évaluer l'algorithme
  // et test pour envoyer le résultat sur des données réelles

  val dataframes = df.randomSplit(Array(0.7, 0.2, 0.1))
  var df_train = dataframes(0)
  var df_dev = dataframes(1)
  var df_test = dataframes(3)

  println(df_train.count())
  println(df_dev.count())
  println(df_test.count())


}