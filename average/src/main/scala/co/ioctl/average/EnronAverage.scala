package co.ioctl.average

import org.apache.spark.sql.SparkSession

/**
  * Calculates the average length in words of emails
  * @param sparkSession
  */
class EnronAverage(sparkSession: SparkSession) {
  import org.apache.spark.sql.DataFrame

  /**
    * This method accepts a path to the the file containing the enron email data. It will prepare the data by removing all elements
    * from the dataset which is not a word
    *
    * @param inputFile the initial dataset
    * @return A dataframe of the data prepared
    */
  def prepareData(inputFile: String): DataFrame = {
    import sparkSession.sqlContext.implicits._

    val pattern = """(\p{Alnum})+""".r // regular expression which will retrieve only alpha numeric filtering out everything else

    val data = sparkSession.sparkContext
      .textFile(inputFile)
      .flatMap(_.split(" "))
      .map(x => pattern.findFirstIn(x).getOrElse(x))
      .map(s => (s, s.length))
      .filter({ case (_, y) => y > 0 })
      .distinct()
      .toDF("word", "length")

    data
  }

  /**
    * Returns the average word count from the dataframe
    *
    * @param df A prepared dataframe
    * @return The average word length
    */
  def average(df: DataFrame): Double = {
    df.createOrReplaceTempView("word_count")

    val totalWordCount = sparkSession.sql("SELECT SUM(length) AS word_length FROM word_count")
      .head
      .getLong(0) * 1.0

    totalWordCount / df.count
  }
}

object EnronAverage extends App {
  import org.apache.spark.{SparkConf, SparkContext}

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Enron average word count")
  val sc = new SparkContext(sparkConf)
  val spark = SparkSession.builder.appName("enron_average_word_count").getOrCreate()

  val averageWordCount = new EnronAverage(spark)
  val df = averageWordCount.prepareData(args(0))
  val averageWordLength = averageWordCount.average(df)

  println(s"Average word count is $averageWordLength")
  sc.stop()
}
