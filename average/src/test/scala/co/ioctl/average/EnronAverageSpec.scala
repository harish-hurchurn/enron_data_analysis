package co.ioctl.average

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FreeSpec, Matchers}

class EnronAverageSpec extends FreeSpec with Matchers with DataFrameSuiteBase {
  val dataPath = "average/src/test/resources/email_data/allen-p/inbox"

  "The EnronAverage job" - {
    "should be able to load email data and return a dataframe of the loaded data " in {
      val enronAverage = new EnronAverage(spark)
      val df = enronAverage.prepareData(dataPath)

      df.count shouldBe 4084
    }

    "should calculate the average word length" in {
      val enronAverage = new EnronAverage(spark)
      val avarageDf = enronAverage.prepareData(dataPath)
      enronAverage.average(avarageDf) shouldBe 6.182664054848188
    }
  }
}
