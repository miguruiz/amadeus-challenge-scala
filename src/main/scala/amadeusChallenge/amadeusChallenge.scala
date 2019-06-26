package amadeusChallenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import java.nio.file.{Paths, Files}
import scala.util.Try


object amadeusChallenge {

  def main(args: Array[String]): Unit = {

    /*
    val bookingsPath: String = "../../Data/challenge_scala/bookings_testing.csv"
    val searchesPath: String = "../../Data/challenge_scala/searches_testing.csv"
    */


    if (args.length != 2){
      println("Error, two parameters were expected")
      return
    }

    // Assign arguments to corresponding variables
    val bookingsPath: String = obtainPath(args, "bookings")
    val searchesPath: String = obtainPath(args, "searches")


    //Validate if files exists
    if (!(Files.exists(Paths.get(bookingsPath)) && Files.exists(Paths.get(searchesPath)))){
      println("Error, at least one of the files does not exist.")
      return
    }

    // Initialize SparkContet & Spark Session
    val sc = new SparkContext("local", "amadeus")
    val spark = SparkSession.builder.appName("Amadeus").getOrCreate()


    //Validate if column exist
    val bookingsColumns = List ("act_date","arr_port","pax","cre_date","dep_port","arr_port")
    val searchesColumns = List ("Date","Origin","Destination")

    val BookingsValidation = validateColumns(bookingsPath,bookingsColumns,spark)
    val SearchesValidation = validateColumns(searchesPath,searchesColumns,spark)

    if (!(BookingsValidation && SearchesValidation)){
      println("Error. columns not found")
      return
    }


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
    * Returns the corresponding args value, if a provided substring is found
    */
  def obtainPath(args: Array[String], subString: String): String = {
    if (args(0).contains(subString))
      args(0)
    else if (args(1).contains(subString))
      args(1)
    else
      s"Error: $subString Not found."
  }

  /**
    * Validates if the given list of columns exists inside the given Dataframe
    */
  def validateColumns(filePath: String, columns: List[String], spark: SparkSession): Boolean = {

    val df = exerciseOne.readFile(filePath,spark)
    val dfClean = exerciseTwo.cleanColumnNames(df)
    val validation = columns.flatMap(c => Try(dfClean(c)).toOption).length

    columns.length == validation
  }


}
