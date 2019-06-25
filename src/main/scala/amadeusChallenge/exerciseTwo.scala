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

  private val AppName = "AmadeusExerciseTwo"
  val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"


  /*
   * Given bookings.csv, creates a lis of arrival airports sorted by passangers (pax);
   *  prints top 10 airprots, and returns the dataframe.
   */
  def topAirports (filePath: String, spark: SparkSession, sc: SparkContext): Unit ={

    // Create Spark Session
    //val spark = SparkSession.builder.appName(AppName).getOrCreate()

    val dfFile = exerciseOne.readFile(filePath, spark)

    //Clean column names
    val dfRenamed =  cleanColumnNames(dfFile)

    // Selecting act_date and arr_port
    val dfSel =dfRenamed.select("act_date", "arr_port", "pax")

    // Filter year 2013
    val dfSel2013 = dfSel.filter(dfSel("act_date").contains("2013"))

    //Strip column "arr_port" - for merging with OpenDataTravel
    val defSel2013Clean = dfSel2013.withColumn("arr_port", trim(col("arr_port")))

    // Group by airport adding passangers and sort by num of pax
    val topAirports = defSel2013Clean.groupBy("arr_port")
      .agg(sum("pax").alias("pax_sum"))
      .sort(desc("pax_sum"))

    //Joining top 10 airports dataframe with GeoBases information

    val iataNames = getAirportNames(sc)

    val topAirportsNames = topAirports.join(iataNames,
      defSel2013Clean.col("arr_port") === iataNames.col("IATA_code"))

    topAirportsNames.sort(desc("pax_sum")).show(10)

    topAirportsNames
  }

  /*
    * Access Geobases to return a list of IATA Airport codes with the corresponding names.
    */
  def getAirportNames (sc:SparkContext, url: String = url): DataFrame = {


    // Create Spark Session
    val spark = SparkSession.builder.appName(AppName).getOrCreate()
    import spark.implicits._  // (!) - tbc

    //Parse URL to RDD
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

    // Selecting columns IATA code and Airport Name
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


}
