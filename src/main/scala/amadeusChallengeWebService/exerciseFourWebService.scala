/**
  * Name: exrciseFourWebService.scala
  *
  * Description:
  *   Web service
  *
  */
package amadeusChallengeWebService

import amadeusChallenge.exerciseTwo
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._


import org.scalatra.ScalatraServlet

class exerciseFourWebService extends ScalatraServlet with JacksonJsonSupport{

  // Initialize SparkContet & Spark Session
  val sc = new SparkContext("local", "amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  //File path -hardcoded
  val bookingsPath: String = "../../Data/challenge_scala/bookings_testing.csv"

  //Execute exercise two and return the dataframe (without validations)
  val df = exerciseTwo.execute(bookingsPath, spark, sc)

  protected implicit lazy val jsonFormats: Formats = DefaultFormats


  get("/") {
    "Test!"
  }

  get("/top/:num") {
    val n = params("num").toInt
    df.limit(n).first() // Pending parse multi-line Json to single-line

  }

}
