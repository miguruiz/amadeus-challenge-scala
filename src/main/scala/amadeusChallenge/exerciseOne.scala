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
import org.apache.spark.sql.SparkSession
import java.text.NumberFormat.getIntegerInstance



object exerciseOne {

  private val AppName = "AmadeusExerciseOne"


  /*
   * name - countLines
   * desc - prints the number of lines and unique lines inside a given file
   */
  def countLines (filePath: String): Unit ={

    // Create a SparkContext object
    val sc = new SparkContext("local",AppName)

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

    // stop sparkContext
    sc.stop()

  }

  /*
   * name - removeDuplicates
   * desc - creates a file without duplicated lines from a given file
    */

  def removeDuplicates (filePath: String, delimiter: String = "^", header: String = "true"): String ={

    // Create a SparkContext object
    val spark = SparkSession.builder.appName(AppName).getOrCreate()

    // Read file into DataFrame
    val dfFile = spark.read
      .option("delimiter", delimiter)
      .option ("header",header)
      .csv(filePath)

    // Remove duplicate
    val dfFileUnique = dfFile.distinct()

    //Creating the name of the new path by removing extension and
    val fileNewPath = filePath.dropRight(4) + "_unique.csv"

    //Save to file using same delimiter and header
    dfFileUnique
      .coalesce(1)
      .write.format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .save(fileNewPath)

    // StopSpark Session
    spark.stop()

    //Return the new filepath
    fileNewPath

  }
}
