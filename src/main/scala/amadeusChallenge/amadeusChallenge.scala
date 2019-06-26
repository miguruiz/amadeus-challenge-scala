package amadeusChallenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


object amadeusChallenge {

  def main(args: Array[String]): Unit = {

    //Path Variables
    val filePath_Bookings: String = "../../Data/challenge_scala/bookings_testing.csv"
    val filePath_Searches: String = "../../Data/challenge_scala/searches_testing.csv"

    val argsLength = args.length

    if (argsLength != 2) {
      println(s"Two parameters were expected, $argsLength were received")
      return
    }

    val (bookingsFilePath,searchesFilePath) = namesCheck(args)

    if (bookingsFilePath == "Error" || searchesFilePath == "Error"){
      println(s"bookings.csv and searches.csv were expected, but not received")
      return
    }


    // Initialize SparkContet & Spark Session
    val sc = new SparkContext("local","amadeus")
    val spark = SparkSession.builder.appName("Amadeus").getOrCreate()


    //Exercise One - count Lines
    val (bookingsUniquePath, searchesUniquePath)  = exerciseOne.execute(filePath_Bookings,filePath_Searches,spark)

    //Exercise Two - top 10 airports
    exerciseTwo.execute(bookingsUniquePath,spark, sc)

    //Exercise Three - searches that created bookings
    exerciseThree.execute(bookingsUniquePath,searchesUniquePath,spark)

    sc.stop()
    spark.stop()
  }

  /**
    * Checks if the provided filenames contain booking and searches.
    */
  def namesCheck(args: Array[String]): (String, String)={
    val bookingsFilePath = {
      if (args(0).contains("booking"))
        args(0)
      else if (args(1).contains("booking"))
        args(1)
      else
        "Error"
    }

    val searchesFilePath = {
      if (args(0).contains("searches"))
        args(0)
      else if (args(1).contains("searches"))
        args(1)
      else
        "Error"
    }
    (bookingsFilePath,searchesFilePath)
  }

  /**
    * Checks if Booking exists, is not empty, and contains the colums used in the application.
    */
  def qualityBookingsCheck(args: Array[String]): String ={
    "Pending"
  }

  /**
    * Checks if Booking exists, is not empty, and contains the colums used in the application.
    */
  def qualitySearchesCheck(args: Array[String]): String ={
    "Pending"
  }


}
