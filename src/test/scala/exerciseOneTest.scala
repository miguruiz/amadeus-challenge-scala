package amadeusChallenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext


/**
  * Test: countLines
  * Desc: Unit tests for amadeusChallenge - exerciseOne - countLines
  */

class exerciseOneTest extends FunSuite with BeforeAndAfter {

  val testFilePath = "../../Data/challenge_scala/bookings_testing.csv"
  val uniqueFilePath = "../../Data/challenge_scala/bookings_testing_unique_values.csv"

  val sc = new SparkContext("local", "amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  var df: DataFrame = _
  var df2: DataFrame = _

  before {

    df = spark.read
      .option("delimiter", "^")
      .option("header", true)
      .csv(testFilePath)

    df2 = spark.read
      .option("delimiter", "^")
      .option("header", true)
      .csv(uniqueFilePath)
  }

  /**
    * Test: countLines
    */

  test("Testing countLines" ) {
    val actual = exerciseOne.countLines(df)
    val expected = (100004, 99999)

    assert(actual === expected)
  }

  /**
    * Test: removeDuplicates
    */

  test("Testing removeDuplicates" ) {
    val actual = exerciseOne.removeDuplicates(df).count()
    val expected = df2.count()

    assert(actual === expected)
  }







}

