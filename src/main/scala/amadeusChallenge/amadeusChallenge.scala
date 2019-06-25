package amadeusChallenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


object amadeusChallenge {

  def main(args: Array[String]): Unit = {

    //Path Variables
    val filePath_Bookings: String = "../../Data/challenge_scala/bookings.csv"
    val filePath_Searches: String = "../../Data/challenge_scala/searches.csv"

    /*
      VERIFY THAT FIRST FILE IS BOOKINGS, SECOND IS SEARCHES
     */


    // Initialize SparkContet & Spark Session
    val sc = new SparkContext("local","amadeus")
    val spark = SparkSession.builder.appName("Amadeus").getOrCreate()


    //Exercise One - count Lines
    exerciseOne.countLines(filePath_Bookings, sc)
    exerciseOne.countLines(filePath_Searches, sc)

    //Exercise One - remove duplicates
    val filePath_Bookings_unique = exerciseOne.removeDuplicates(filePath_Bookings, spark)
    val filePath_Searches_unique= exerciseOne.removeDuplicates(filePath_Searches, spark)

    //Exercise Two - Top Airports
    exerciseTwo.topAirports(filePath_Bookings_unique,spark,sc)

    //Exercise Three - Match airport with bookings
    exerciseThree.mergeSearchesBookings(filePath_Bookings_unique,filePath_Searches_unique,spark)

    sc.stop()
  }

}



/*
CONSIDERATIONS:
- Probably having sparkSession and SparkContet in the same application is uneeded.

*/
