package amadeusChallenge

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext


/**
  * Test: countLines
  * Desc: Unit tests for amadeusChallenge - exerciseOne - countLines
  */

class exerciseOneTest extends FunSuite with BeforeAndAfter {

  val testFilePath = "../../Data/challenge_scala/bookings_testing.csv"
  val sc = new SparkContext("local", "amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  var df: DataFrame = _

  before {

    df = spark.read
      .option("delimiter", "^")
      .option("header", true)
      .csv(testFilePath)
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
    * Test: readFile
    */


  test("Testing function readFile" ) {

    val df = spark.read
      .option("delimiter", "^")
      .option("header", true)
      .csv(testFilePath)

    {

    assert(isinstance(df,DataFrame))
  }





}

