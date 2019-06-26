/**
  *
  * help_link - https://github.com/bosea/spark-unit-testing
  */
package amadeusChallenge


import com.holdenkarau.spark.testing.SharedSparkContext
import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}


/**
  * Unit tests for ml.dolphin.testing.DistanceFromCentroid methods
  *
  * @author Abhijit Bose
  * @version 1.0 11/24/2015
  * @since 1.0 11/24/2015
  */

/*
class exerciseOneTest extends FunSuite with BeforeAndAfter with  SharedSparkContext {

  val testFilePath = "../../Data/challenge_scala/bookings_testing.csv"

  var vPoints: Array[Vector] = _
  var centroid: Vector = _
  var vPointsRdd: RDD[Vector] = _

  before {
    val df = spark.read
      .option("delimiter", "^")
      .option("header", true)
      .csv(testFilePath)
  }

  test("Testing calcDistance using a shared Spark Context") {
    val sum = DistanceFromCentroid.calcDistance(sc, vPointsRdd, centroid)
    val expected = sqrt(14.0) + sqrt(30.0) + sqrt(104.0) + sqrt(90.0)
    assert(sum === expected)
  }

}







/*
import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/*
 * Test: countLines
 */
final class exerciseOneTest extends FlatSpec with Matchers  {
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



}

*/
*/
