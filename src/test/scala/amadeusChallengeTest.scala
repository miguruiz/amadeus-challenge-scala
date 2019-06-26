package amadeusChallenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext

class amadeusChallengeTest extends FunSuite with BeforeAndAfter {

  val argsTest = Array ("../../Data/challenge_scala/bookings_testing.csv",
    "../../Data/challenge_scala/searches_testing.csv")
  val subString = "bookings"

  val testFilePath = argsTest(0)

  val sc = new SparkContext("local", "amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  var columns: List [String] = _

  columns = List("act_date", "source",  "pos_ctry", "pos_iata", "pos_oid", "rloc", "cre_date", "duration",
    "distance", "dep_port", "dep_city", "dep_ctry", "arr_port", "arr_city", "arr_ctry", "lst_port", "lst_city",
    "lst_ctry", "brd_port", "brd_city", "brd_ctry", "off_port", "off_city", "off_ctry", "mkt_port", "mkt_city",
    "mkt_ctry", "intl", "route", "carrier", "bkg_class", "cab_class", "brd_time", "off_time", "pax", "year",
    "month", "oid")

  /**
    * Test: obtainPath
    */

  test("Testing obtainPath" ) {
    val actual = amadeusChallenge.obtainPath(argsTest,subString)
    val expected = "../../Data/challenge_scala/bookings_testing.csv"

    assert(actual === expected)
  }

  /**
    * Test: validateColumns
    */

  test("Testing validateColumns" ) {
    val actual = amadeusChallenge.validateColumns(testFilePath, columns , spark)
    val expected = true

    assert(actual === expected)
  }

}



