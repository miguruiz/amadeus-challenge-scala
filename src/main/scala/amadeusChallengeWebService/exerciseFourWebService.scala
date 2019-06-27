package amadeusChallengeWebService

import amadeusChallenge.exerciseTwo
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.scalatra.ScalatraServlet

class exerciseFourWebService extends ScalatraServlet{
/*
  // Initialize SparkContet & Spark Session
  val sc = new SparkContext("local", "amadeus")
  val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

  //File path -hardcoded
  val bookingsPath: String = "../../Data/challenge_scala/bookings_testing.csv"

  //Execute exercise two and return the dataframe (without validations)
  val df = exerciseTwo.execute(bookingsPath, spark, sc)
*/

  get("/") {
    "Scalatra rules!"
  }

  get("/top/:fname") {
    val n = params("fname").toInt
    s"Hello, the number selected is ${n.isInstanceOf}"
    //df.limit(n).toJSON.toString()
  }

  sc.stop()
  spark.stop()


}
