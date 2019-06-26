package amadeusChallenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import java.nio.file.{Paths, Files}


object amadeusChallenge {

  def main(args: Array[String]): Unit = {

    //Path Variables
    val filePath_Bookings_temp: String = "../../Data/challenge_scala/bookings_testing.csv"
    val filePath_Searches_temp: String = "../../Data/challenge_scala/searches_testing.csv"

    if (args.length == 2){
      println("Error, two parameters were expected")
      return
    }

    // Assign arguments to correspomding variables
    val bookingsPath: String = obtainPath(args, "bookings")
    val searchesPath: String = obtainPath(args, "searches")


    //Validate if files exists
    if (!(Files.exists(Paths.get(bookingsPath)) && Files.exists(Paths.get(searchesPath)))){
      println("Error, at least one of the files have not been found")
      return
    }


    //Validate if column exist

    


    // Initialize SparkContet & Spark Session
    val sc = new SparkContext("local", "amadeus")
    val spark = SparkSession.builder.appName("Amadeus").getOrCreate()


    //Exercise One - count Lines
    val (bookingsUniquePath, searchesUniquePath) = exerciseOne.execute(bookingsPath, searchesPath, spark)

    //Exercise Two - top 10 airports
    exerciseTwo.execute(bookingsUniquePath, spark, sc)

    //Exercise Three - searches that created bookings
    exerciseThree.execute(bookingsUniquePath, searchesUniquePath, spark)

    sc.stop()
    spark.stop()
  }


  /**
    * Returns true if bookings.csv and searches.csv have been included as parameters
    */
  def obtainPath(args: Array[String], subString: String): String = {
    if (args(0).contains(subString))
      args(0)
    else if (args(1).contains(subString))
      args(0)
    else
      s"Error: $subString Not found."
  }


}
