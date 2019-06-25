package amadeusChallenge

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, substring, trim}

object exerciseThree {

  /*
   * Given Bookings.csv and Searches.csv, creates a new file similar to Searches.csv with an additional column "booking"
   * that contains value "1" if the searched finished in a booking, and "0" if not.
   */
  def mergeSearchesBookings(filePath_Bookings: String, filePath_Searches:String, spark: SparkSession): Unit = {

    val dfBookingsTemp = exerciseOne.readFile(filePath_Bookings, spark)
    val dfSearchesTemp = exerciseOne.readFile(filePath_Searches, spark)

    //Clean column names in both dataframes

    val dfBookings = exerciseTwo.cleanColumnNames(dfBookingsTemp)
    val dfSearches = exerciseTwo.cleanColumnNames(dfSearchesTemp)

    //Adding bookings column with "ones" to Bookings
    val dfBookingsBin = dfBookings.withColumn("booking", lit(1))

    //Adding index column to searches
    val dfSearchesSelIdx = dfSearches.withColumn("index", monotonically_increasing_id())

    /*
      PENDING - Cleaning for Nulls.
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

    //New file path
    val fileNewPath = filePath_Searches.dropRight(4) + "_with_bookings.csv"

    exerciseOne.saveFile(searchesFinal,fileNewPath,spark)

  }

}
