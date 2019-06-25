package amadeusChallenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, substring, trim}

object exerciseThree {

  /*
   * name - countLines
   * desc - prints the number of lines and unique lines inside a given file
   */
  def mergeSearchesBookings(filePath_Bookings: String, filePath_Searches:String, spark: SparkSession): Unit = {

    val dfBookingsTemp = readFile(filePath_Bookings, spark)
    val dfSearchesTemp = readFile(filePath_Searches, spark)

    //Clean column names in both dataframes

    val dfBookings = cleanColumnNames(dfBookingsTemp)
    val dfSearches = cleanColumnNames(dfSearchesTemp)

    //Adding bookings column with "ones" to Bookings
    val dfBookingsBin = dfBookings.withColumn("booking", lit(1))

    //Adding index column to searches
    val dfSearchesSelIdx = dfSearches.withColumn("index", monotonically_increasing_id())

    /*

    PENDING - Cleaning for Nulls

     */

    //Selecting only the necessary columns
    val dfSearchesSelIdxSel = dfSearchesSelIdx.select("Origin","Destination", "Date", "index")
    val dfBookingsBinSel = dfBookingsBin.select("arr_port","dep_port", "cre_date", "booking")

    // Clean date on Bookings & stripping airport columns & remove duplicates
    val dfBookingsBinSelRdy = dfBookingsBinSel
      .withColumn("cre_date", substring(col("cre_date"),1,10) )
      .withColumn("dep_port", trim(col("dep_port")))
      .withColumn("arr_port", trim(col("arr_port")))
      .distinct()

    // Clean columns Origin & Destination in Searches
    val dfSearchesSelIdxSelRdy = dfSearchesSelIdxSel.withColumn("Origin", trim(col("Origin")))
      .withColumn("Destination", trim(col("Destination")))

    // Merge files Bookings with Searches on the Date, flight origin and destination
    val searchesWithBookings = dfSearchesSelIdxSelRdy.join(dfBookingsBinSelRdy,
      dfSearchesSelIdxSelRdy.col("Date") === dfBookingsBinSelRdy.col("cre_date") &&
        dfSearchesSelIdxSelRdy.col("Origin") === dfBookingsBinSelRdy.col("dep_port") &&
        dfSearchesSelIdxSelRdy.col("Destination") === dfBookingsBinSelRdy.col("arr_port"), "left")

    // Merge column "bookings" to the original Searches file
    val searchesWithBookings_2 = searchesWithBookings.select("index","booking")
    val SearchesOriginalWithBookings = dfSearchesSelIdx.join(searchesWithBookings_2,
      dfSearchesSelIdx.col("index")===searchesWithBookings_2.col("index"), "left" ).drop("index")

    //Fill nulls in column "booking" with value 0, and remove column index
    val searchesFinal = SearchesOriginalWithBookings.na.fill(0,Seq("booking"))
    searchesFinal.show()
  }

  def readFile(filePath: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("delimiter", "^")
      .option("header", "true")
      .csv(filePath)
  }

  def cleanColumnNames (df:DataFrame): DataFrame = {
    //Clean column names in both dataframes
    val newColumnNamesBookings = df.columns.map(_.replace(" ", ""))

    //Creating new dataframe with cleaned column names
    df.toDF(newColumnNamesBookings: _*)
  }

  def saveToFile (df:DataFrame, filePath:String): Unit = {
    //Creating the name of the new path by removing extension and
    val fileNewPath = filePath.dropRight(4) + "_with_bookings.csv"

    //Save Results to file
    df
      .coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .option("delimiter", "^")
      .save(fileNewPath)

  }


}
