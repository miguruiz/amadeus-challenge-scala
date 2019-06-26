/*
 * Name: exerciseOne.scala
 * Description: Count lines and
 *
 */
package amadeusChallenge

// import required  classes

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.text.NumberFormat.getIntegerInstance


object exerciseOne {

  private val AppName = "AmadeusExerciseOne"

  /*
   * Executes exercise one (counting lines, and removing duplicates), and returns the name of the new files.
   */
  def execute (filePathBookings: String, filePathSearches: String, spark: SparkSession): (String, String) ={

    //Read files
    val dfBookings = readFile(filePathBookings, spark)
    val dfSearches = readFile(filePathSearches, spark)

    //Calculate lines for Bookings
    val bookingsLines = countLines(dfBookings)
    val searchesLines = countLines(dfSearches)

    //Print Results
    printResults(bookingsLines._1,bookingsLines._2,filePathBookings )
    printResults(searchesLines._1, searchesLines._2,filePathSearches)

    //Remove duplicates
    val dfBookingsUnique = removeDuplicates(dfBookings)
    val dfSearchesUnique = removeDuplicates(dfSearches)

    //Save to file
    val bookingsUniquePath = saveFile(dfBookingsUnique,filePathBookings, "_unique.csv", spark)
    val searchesUniquePath = saveFile (dfSearchesUnique,filePathSearches, "_unique.csv", spark )

    // Return new file paths
    (bookingsUniquePath, searchesUniquePath)
  }

  /*
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

  /*
  * Given a dataframe, counts total and unique lines, and returns a touple with total and unique lines
  */
  def countLines (df: DataFrame): (Long, Long) ={

    val totalLines = df.count()
    val uniqueLines = df.distinct().count()

    (totalLines,uniqueLines)
  }


  /*
   * Given a file, removes duplicates.
   */

  def removeDuplicates (df: DataFrame): DataFrame ={
    df.distinct()
  }

  /*
   * Reads a file into dataframe, and returns the dataframe
   */

  def readFile(filePath: String,
               spark: SparkSession,
               delimiter: String = "^",
               header:Boolean=true): DataFrame = {
    spark.read
      .option("delimiter", delimiter)
      .option("header", header)
      .csv(filePath)
  }

  /*
   * Save a file to csv, returns the new file path
   */
  def saveFile(df: DataFrame,
               filePath: String,
               extension: String,
               spark: SparkSession,
               delimiter: String = "^",
               header:Boolean=true
              ): String ={

    //Creating the name of the new path by removing extension and
    val fileNewPath = filePath.dropRight(4) + extension

    //Save to file; overwrite if exist
    df
      .coalesce(1)
      .write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", header)
      .option("delimiter", delimiter)
      .save(fileNewPath)

    fileNewPath
  }
}

