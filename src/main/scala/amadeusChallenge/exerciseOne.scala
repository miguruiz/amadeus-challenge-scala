/*
 * Name: exerciseOne.scala
 * Description:
 *
 * Pending?
 *  - Am I using the sparkContect corretly?
 *  - File name of
 *
 * TESTS:
 *  - ensure that we receive .csv files
 */
package amadeusChallenge

// import required  classes

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.NumberFormat.getIntegerInstance



object exerciseOne {

  private val AppName = "AmadeusExerciseOne"


  /*
   * Counts and print the unique and total lines from a given filepath
   */
  def countLines (filePath: String, sc: SparkContext): Unit ={

    // Read file to RDD
    val fileRdd = sc.textFile(filePath)

    //Count total lines
    val totalLines = fileRdd.count()

    //Count unique lines - excluding duplicates
    val uniqueLines = fileRdd.distinct().count()

    //val formatter = java.text.NumberFormat.getIntegerInstance
    val totalLinesFormatted = getIntegerInstance.format(totalLines)
    val uniqueLinesFormatted = getIntegerInstance.format(uniqueLines)

    //Printing results
    println(s"TOTAL LINES IN $filePath")
    println("------------------------------")
    println("")
    println(s"   Total lines : $totalLinesFormatted")
    println(s"   Unique lines:  $uniqueLinesFormatted")

  }

  /*
   * Given a file, creates a new one without duplicated lines. Returns the new filepath.
   */

  def removeDuplicates (filePath: String, spark: SparkSession): String ={

    // Read file into DataFrame
    val dfFile = readFile(filePath, spark)

    // Remove duplicate
    val dfFileUnique = dfFile.distinct()

    //Creating the name of the new path by removing extension and
    val fileNewPath = filePath.dropRight(4) + "_unique.csv"

    //Save to file using same delimiter and header
    saveFile(dfFileUnique,fileNewPath, spark)

    fileNewPath

  }

  /*
   * Reads a file into dataframe, and returns the dataframe
   */

  def readFile(filePath: String, spark: SparkSession, delimiter: String = "^", header:Boolean=true): DataFrame = {
    spark.read
      .option("delimiter", delimiter)
      .option("header", header)
      .csv(filePath)
  }

  /*
   * Save a file to csv
   */
  def saveFile(df: DataFrame, filePath: String, spark: SparkSession, delimiter: String = "^", header:Boolean=true): Unit ={
    df
      .coalesce(1)
      .write.format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .save(filePath)

  }
}


/*
CONSIDERATIONS:
- Probably having sparkSession and SparkContet in the same application is uneeded.

*/