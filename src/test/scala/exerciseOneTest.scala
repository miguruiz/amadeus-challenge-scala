package amadeusChallenge

import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


final class exerciseOneTest extends FlatSpec with Matchers  {
  val testFilePath = "../../Data/challenge_scala/bookings_testing.csv"
  val sc = new SparkContext("local","amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  val df = spark.read
    .option("delimiter", "^")
    .option("header", true)
    .csv(testFilePath)

  "CountLines" should "count the lines in a file" in {
    exerciseOne.countLines(df) shouldBe (100004,99999)
  }

}