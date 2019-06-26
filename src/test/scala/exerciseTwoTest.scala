package amadeusChallenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext


/**
  * Unit tests for amadeusChallenge - exerciseTwo - cleanColumnNames function
  */

class exerciseTwoTest extends FunSuite with BeforeAndAfter {

  val testFilePath = "../../Data/challenge_scala/bookings_testing.csv"
  val sc = new SparkContext("local", "amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  var columns: Array [String] = _
  var df: DataFrame = _

  before {

    columns = Array("act_date", "source",  "pos_ctry", "pos_iata", "pos_oid", "rloc", "cre_date", "duration",
      "distance", "dep_port", "dep_city", "dep_ctry", "arr_port", "arr_city", "arr_ctry", "lst_port", "lst_city",
      "lst_ctry", "brd_port", "brd_city", "brd_ctry", "off_port", "off_city", "off_ctry", "mkt_port", "mkt_city",
      "mkt_ctry", "intl", "route", "carrier", "bkg_class", "cab_class", "brd_time", "off_time", "pax", "year",
      "month", "oid")

    df = spark.read
      .option("delimiter", "^")
      .option("header", true)
      .csv(testFilePath)
  }

  test("Testing function cleanColumnNames" ) {
    val actual = exerciseTwo.cleanColumnNames(df).columns
    val expected = columns

    assert(actual === expected)
  }





}