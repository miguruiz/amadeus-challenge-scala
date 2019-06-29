/**
  * Name: exerciseOne.scala - reading files
  *
  * Description:
  *   Check the number of lines in each of the two files (bookings and searches)
  *
  */
package amadeusChallenge

// import required  classes

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.text.NumberFormat.getIntegerInstance


object exerciseOne {

  private val AppName = "AmadeusExerciseOne"

  /**
   * Executes exercise one (counting lines, and removing duplicates), and returns the name of the new files.
   */
  def execute (filePathBookings: String, filePathSearches: String, spark: SparkSession): (String, String) ={


    //Read files
    val dfBookings = myFunctions.readFile(filePathBookings, spark)
    val dfSearches = myFunctions.readFile(filePathSearches, spark)

    //Calculate lines for Bookings
    val bookingsLines = myFunctions.countLines(dfBookings)
    val searchesLines = myFunctions.countLines(dfSearches)

    //Print Results
    printResults(bookingsLines._1,bookingsLines._2,filePathBookings )
    printResults(searchesLines._1, searchesLines._2,filePathSearches)

    //Remove duplicates
    val dfBookingsUnique = myFunctions.removeDuplicates(dfBookings)
    val dfSearchesUnique = myFunctions.removeDuplicates(dfSearches)

    //Save to file
    val bookingsUniquePath = myFunctions.saveFile(dfBookingsUnique,filePathBookings, "_unique.csv", spark)
    val searchesUniquePath = myFunctions.saveFile (dfSearchesUnique,filePathSearches, "_unique.csv", spark )

    // Return new file paths
    (bookingsUniquePath, searchesUniquePath)
  }

  /**
  * Prints total and unique lines results
  */
  def printResults (totalLines: Long, uniqueLines: Long, filePath: String): Unit ={

    //val formatter = java.text.NumberFormat.getIntegerInstance
    val totalLinesFormatted = getIntegerInstance.format(totalLines)
    val uniqueLinesFormatted = getIntegerInstance.format(uniqueLines)

    println(s"TOTAL LINES IN $filePath")
    println("------------------------------")
    println("")
    println(s"   Total lines : $totalLinesFormatted")
    println(s"   Unique lines:  $uniqueLinesFormatted")
    println("")
  }


}

