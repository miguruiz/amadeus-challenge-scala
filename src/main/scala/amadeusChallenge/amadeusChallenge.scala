package amadeusChallenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


object amadeusChallenge {

  def main(args: Array[String]): Unit = {

    //Path Variables
    val filePath_Bookings: String = "../../Data/challenge_scala/bookings_testing.csv"
    val filePath_Searches: String = "../../Data/challenge_scala/searches_testing.csv"

    /*
      VERIFY THAT FIRST FILE IS BOOKINGS, SECOND IS SEARCHES
     */

    // Initialize SparkContet & Spark Session
    //val sc = new SparkContext("local","amadeus")
    val spark = SparkSession.builder.appName("Amadeus").getOrCreate()


    //Exercise One - count Lines
    val uniquePaths = exerciseOne.execute(filePath_Bookings,filePath_Searches,spark)

    
    spark.stop()
  }


}


/*
CONSIDERATIONS:
- Probably having sparkSession and SparkContet in the same application is uneeded.
*/
