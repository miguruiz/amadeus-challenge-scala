package amadeusChallenge

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object myFunctions {

  val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"


  /**
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


  /**
    * Given a file, removes duplicates.
    */

  def removeDuplicates (df: DataFrame): DataFrame ={
    df.distinct()
  }

  /**
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

  /**
    * Given a dataframe, counts total and unique lines, and returns a touple with total and unique lines
    */
  def countLines (df: DataFrame): (Long, Long) ={

    val totalLines = df.count()
    val uniqueLines = df.distinct().count()

    (totalLines,uniqueLines)
  }

  /**
    * Adds iata airport names to a givendataframe.
    */
  def includeAirportNames (topAirports: DataFrame,
                           spark:SparkSession,
                           url: String = url): DataFrame = {

    val iataNames = getAirportNames(spark)

    //Merging airport names with existing datagrame
    val topAirportsNames = topAirports.join(iataNames,
      topAirports.col("arr_port") === iataNames.col("IATA_code"))

    //Sort and return dataframe with airport names
    topAirportsNames.sort(desc("pax_sum"))

  }

  /**
    * Returns a dataframe with Iata code and airport names from Geobases
    */

  def getAirportNames (spark: SparkSession): DataFrame = {
    import spark.implicits._  // (!) - tbc

    //Parse URL from opentraveldata- geobases to RDD
    val geoContent = scala.io.Source.fromURL(url).mkString
    val geoContentList = geoContent.split("\n").filter(_ != "")
    val geoContentRdd = spark.sparkContext.parallelize(geoContentList)

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

  /**
    * Cleanse the column names of a dataframe
    */

  def cleanColumnNames (df:DataFrame): DataFrame = {
    //Clean column names in both dataframes
    val newColumnNamesBookings = df.columns.map(_.replace(" ", ""))
    //Creating new dataframe with cleaned column names
    df.toDF(newColumnNamesBookings: _*)
  }


  /**
    * Returns dataframe without null values
    */

  def cleanNulls (df:DataFrame): DataFrame = {
    df.na.drop(how = "any")

  }

}
