package co.ioctl.top100

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class EnronTop100RecepientsSpec extends FreeSpec with BeforeAndAfter with Matchers with DataFrameSuiteBase {
  import java.io.File

  import org.apache.commons.io.FileUtils

  before(FileUtils.deleteDirectory(new File(outputPath)))
  after(FileUtils.deleteDirectory(new File(outputPath)))

  val inputPath = "top100/src/test/resources/email_data/input"
  val outputPath = "top100/src/test/resources/email_data/output/"

  "The EnronTop100Recepients job" - {
    "should be able to read an XML which contains TO and CC information and and convert this file to two CSV files and save these files to the output directory" in {
      spark.sparkContext.setLogLevel("WARN")

      val toAndCcCount = new EnronTop100Recepients(spark)
      val output: Unit = toAndCcCount.convertFileToCSV(inputPath, outputPath)
      new java.io.File(outputPath).exists should not be false
    }

    "should be able to process the CSV files which contains the TO and CC emails and return all of the data" in {
      spark.sparkContext.setLogLevel("WARN")

      spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

      val toAndCcCount = new EnronTop100Recepients(spark)

      toAndCcCount.convertFileToCSV(inputPath, outputPath)
      val data = toAndCcCount.prepareData(outputPath)

      data.count shouldBe 11235
    }

    "should have correct SQL" in {
      spark.sparkContext.setLogLevel("WARN")

      spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

      val toAndCcCount = new EnronTop100Recepients(spark)

      toAndCcCount.convertFileToCSV(inputPath, outputPath)
      val data = toAndCcCount.prepareData(outputPath)
      toAndCcCount.top100(data).count shouldBe 2180
    }
  }
}
