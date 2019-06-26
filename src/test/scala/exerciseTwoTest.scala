/*package amadeusChallenge

import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/*
 * Test: countLines
 */
final class exerciseTwoTest extends FlatSpec with Matchers  {
  val testFilePath = "../../Data/challenge_scala/bookings_testing.csv"
  val sc = new SparkContext("local","amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  val df = spark.read
    .option("delimiter", "^")
    .option("header", true)
    .csv(testFilePath)

  "Countlines" should "return total num of lines, and unique lines" in {
    assert(exerciseOne.countLines(df) == (100004,99999))
  }
*/

