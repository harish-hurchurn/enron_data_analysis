package co.ioctl.top100

import org.apache.spark.sql.SparkSession

/**
  * Represents an email with a weighting factor
  *
  * @param emailAddress the email of the user whethere addressed or cc'd
  * @param weightingFactor  the weighting factor
  */
case class Email(emailAddress: String, weightingFactor: Double)

/**
  * Will return the top 100 emails by recipient email addresses
  *
  * @param sparkSession The spark session to be used
  */
class EnronTop100Recepients(sparkSession: SparkSession) {
  import org.apache.spark.sql.DataFrame

  /**
    * From the input path convert the data into two CSV files, which will contain the emailed to data and the cc data
    * It is assumed that the email data is in XML format
    *
    * @param inputPath  The input path of the XML data which contains the to and cc'd data
    * @param outputPath Where to store the CSV files
    */
  def convertFileToCSV(inputPath: String, outputPath: String): Unit = {
    import org.apache.spark.rdd.RDD
    import sparkSession.sqlContext.implicits._

    /**
      * Does the actual work of unpacking the XML and saving the XML to two files as appropriate
      *
      * @param path      where the XML files is stored on disk
      * @param rddData   The rdd of the converted XML data
      * @param filterTag The tags to be searched for. Either #To or #CC
      * @param weightage The weighting to be applied to the saved data
      */
    def saveFile(path: String, rddData: RDD[Seq[(String, String, String)]], filterTag: String, weightage: String): Unit = {
      val pattern = """(\w+)(\s+)(\w+)|(((\p{Alnum})+.(\p{Alnum})+)|((\p{Alnum})+))@(?i)enron.com""".r

      rddData.
        flatMap(a => a) // Unpack the RDD
        .filter(_._2.contains(filterTag)) // Select only the tags which has #To or #CC
        .map(x => x._3) // Get the values (comma separated from the third element
        .flatMap(_.split(","))
        .map(_.trim)
        .map(x => pattern.findFirstIn(x).getOrElse(x))
        .map(_.replaceAll(" +", " "))
        .map(_.concat(weightage)) // assign the weightings
        .saveAsTextFile(path) // Save the output to a file
    }

    val saveCCEmailPath = outputPath + "/CCEmail"
    val saveToEmailPath = outputPath + "/ToEmail"

    import org.apache.spark.sql.Row

    val combinedEmailIds = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Tags")
      .option("valueTag", "_TagName")
      .load(inputPath)

    val rows = combinedEmailIds.select($"Tag")
      .rdd.map(_.getSeq[Row](0))
      .map(_.map { case Row(tagDataType: String, tagName: String, tagValue: String) => (tagDataType, tagName, tagValue) })

    saveFile(saveCCEmailPath, rows, "#CC", ",0.5")
    saveFile(saveToEmailPath, rows, "#To", ",1.0")
  }

  /**
    * Will process the CSV files which accepts the To and CC data and convert this data into a dataframe
    *
    * @param inputFile The path of where the CSV files can be found
    * @return A dataframe which contains the data To and CSV data with weightings
    */
  def prepareData(inputFile: String): DataFrame = {
    import scala.collection.mutable.ListBuffer

    var emails = new ListBuffer[(String, String)]()

    val res = sparkSession.sparkContext
      .wholeTextFiles(s"$inputFile/*/*")
      .map(y ⇒ y._2.split("\n"))
      .map(emailWithWeightings => {

        // Mutable - but this isn't leaked to the outside world
        val array = emailWithWeightings
        var counter = 0

        array.foreach { _ ⇒
          val name = array(counter).substring(0, array(counter).indexOf(","))
          val weighting = array(counter).substring(array(counter).indexOf(",") + 1)

          if (counter == array.length)
            counter = 0
          else {
            counter += 1
            emails += ((name, weighting))
          }
        }

        emails.toList
      })

    import sparkSession.sqlContext.implicits._
    val df = res.flatMap(a ⇒ a.map(b ⇒ Email(b._1, b._2.toDouble))).toDF()
    df
  }

  /**
    * Accepts a dataframe which contains *all* of the data of the To and CC data and will return the top 100 from this
    *
    * @param df a dataframe containing all of the data
    * @return The top 100
    */
  def top100(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("top100")
    val totEmailIdCountDF = sparkSession.sql("SELECT emailAddress, COUNT(weightingFactor) as weighting FROM top100 GROUP BY emailAddress")

    totEmailIdCountDF.createOrReplaceTempView("EmailCount")

    val sortedEmailCountDF= sparkSession.sql("SELECT emailAddress, weighting FROM EmailCount ORDER BY weighting DESC")
    sortedEmailCountDF
  }
}

object EnronTop100Recepients extends App {
  import org.apache.spark.{SparkConf, SparkContext}

  val sparkConf = new SparkConf()
    .setAppName("Top 100 emails")

  val sc = new SparkContext(sparkConf)
  sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

  val spark = SparkSession
    .builder
    .appName("top_100_from_emails")
    .getOrCreate()

  val top100 = new EnronTop100Recepients(spark)

  val outputPath: Unit = top100.convertFileToCSV(inputPath = args(0), outputPath = args(1))
  val df = top100.prepareData(args(1))
  val resultOfTop100 = top100.top100(df)

  println(s"Top 100 is ${resultOfTop100.show(100)}")
  sc.stop()
}