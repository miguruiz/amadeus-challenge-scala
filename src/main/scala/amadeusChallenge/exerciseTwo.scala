/*
 * Name: exerciseTwo.scala
 * Description:
 *
 * Pending?
 *  - How to make more versatile topTenAirports to receive different files but with Date, Airports, Pax
 *  - Remove Nulls
 *  - Mejora... hacer un objeto, que sea TopAirports con atributo DataFrame... y métodos: top-10, y un booleano para los airport names... otro para cities.
 * TESTS:
 * - check that the file is boookings.csv
 *
 */
package amadeusChallenge

// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, sum, trim}


object exerciseTwo {

  val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"


  /*
   * Calculates and prints the top 10 airports by passanger.
   */
  def execute (bookingFilePath: String, spark: SparkSession, sc: SparkContext): Unit ={

    // Create Spark Session
    val dfBookings = exerciseOne.readFile(bookingFilePath, spark)

    //Calculate the top airports
    val dfTopAirports = topAirports(dfBookings)

    //Include airport names
    val dfTopAiportsWithNames = includeAirportNames(dfTopAirports, spark, sc)

    //Print top 10
    printTopN(dfTopAiportsWithNames, 10)

  }

  /*
   * Given bookings.csv, retunrs a dataframe with airports sorted by number of passangers
   */
  def topAirports (dfBookings: DataFrame): DataFrame ={

    //Clean column names
    val dfBookingsClean =  cleanColumnNames(dfBookings)

    // Selecting act_date and arr_port
    val dfSel =dfBookingsClean.select("act_date", "arr_port", "pax")

    // Filter year 2013
    val dfSel2013 = dfSel.filter(dfSel("act_date").contains("2013"))

    //Strip column "arr_port" - for merging with OpenDataTravel
    val defSel2013Clean = dfSel2013.withColumn("arr_port", trim(col("arr_port")))

    // Group by airport adding passangers and sort by num of pax
    defSel2013Clean.groupBy("arr_port")
      .agg(sum("pax").alias("pax_sum"))
      .sort(desc("pax_sum"))
  }

  /*
  * Prints the n top airports
  */
  def printTopN (df: DataFrame, n: Int): Unit ={
    df.show(n)
  }

  /*
    * Adds iata airport names to a givendataframe.
    */
  def includeAirportNames (topAirports: DataFrame,
                           spark:SparkSession,
                           sc:SparkContext,
                           url: String = url): DataFrame = {

    val iataNames = getAirportNames(spark,sc)

    //Merging airport names with existing datagrame
    val topAirportsNames = topAirports.join(iataNames,
      topAirports.col("arr_port") === iataNames.col("IATA_code"))

    //Sort and return dataframe with airport names
    topAirportsNames.sort(desc("pax_sum"))

  }

  /*
  * Returns a dataframe with Iata code and airport names from Geobases
  */

  def getAirportNames (spark: SparkSession, sc: SparkContext): DataFrame = {
    import spark.implicits._  // (!) - tbc

    //Parse URL from opentraveldata- geobases to RDD
    val geoContent = scala.io.Source.fromURL(url).mkString
    val geoContentList = geoContent.split("\n").filter(_ != "")
    val geoContentRdd = sc.parallelize(geoContentList)

    //Split into Columns
    val geoContentRddArrays = geoContentRdd.map(_.split("\\^"))

    // Calculate total number of columns
    val maxCols = geoContentRddArrays.first().length
    val newColNames = Seq("IATA_code", "Airport_name")

    // Converting RDD to Datafrae and giving general names to columns
    val geoContentDf = geoContentRddArrays.toDF("arr")
      .select((0 until maxCols).map(i => $"arr"(i).as(s"col_$i")): _*)

    // Selecting and returning columns IATA code and Airport Name
    geoContentDf.select("col_0","col_1").toDF(newColNames: _*)
  }

  /*
  * Cleanse the column names of a dataframe
  */

  def cleanColumnNames (df:DataFrame): DataFrame = {
    //Clean column names in both dataframes
    val newColumnNamesBookings = df.columns.map(_.replace(" ", ""))

    //Creating new dataframe with cleaned column names
    df.toDF(newColumnNamesBookings: _*)
  }


  /*
 * Returns dataframe without null values
 */

  def cleanNulls (df:DataFrame): DataFrame = {
    df.na.drop(how = "any")
  }



}
