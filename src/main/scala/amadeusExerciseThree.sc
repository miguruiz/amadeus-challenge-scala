/*
 * EXERCISE THREE: IDENTIFYING SEARCHES THAT FINISHED IN BOOKINGS
 * Details:
 *  - For every search in the searches file, find out whether the search ended up in a
 *      booking or not (using the info in the bookings file)
 * Note: Since I already conducted this exercise in Python, I will follow the same approach, which is
 *  to merge by the following criteria:
 *    - "Origin"(searches.csv) == "dep_port" (bookings.csv)
 *    - "Destination" (searches.csv) == "arr_port" (bookings.csv)
 *    - "Date" (searches.csv) == "cre_date" (bookings.csv)
 *
 *  - PENDING:
 *     drop nulls
 */

//PART I: Select top 10 arrival airports

// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit,monotonically_increasing_id, trim, col, substring }


// file_names
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"
val bookingsFileUnique: String = "../dsc/Data/challenge_scala/bookings_unique.csv"
val bookingsFileTesting: String = "../dsc/Data/challenge_scala/bookings_testing.csv"

val searchesFile: String = "../dsc/Data/challenge_scala/searches.csv"
val searchesFileUnique: String = "../dsc/Data/challenge_scala/searches_unique.csv"
val searchesFileTesting: String = "../dsc/Data/challenge_scala/searches_testing.csv"


val fileInUseBookings = bookingsFileUnique
val fileInUseSearches = searchesFileUnique

  // create a SparkContext object
val sc = new SparkContext("local","amadeus-challenge")

// Create Spark Session

val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()
import spark.implicits._

//Import files to dataframe setting header and delimiter

val dfBookingsTemp = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUseBookings)

val dfSearchesTemp = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUseSearches)


//Clean column names in both dataframes
val newColumnNamesBookings = dfBookingsTemp.columns.map(_.replace (" ",""))
val newColumnNamesSearches = dfSearchesTemp.columns.map(_.replace (" ",""))

//Creating new dataframe with cleaned column names
val dfBookings  = dfBookingsTemp.toDF(newColumnNamesBookings: _*)
val dfSearches  = dfSearchesTemp.toDF(newColumnNamesSearches: _*)

//Adding bookings column with "ones" to Bookings
val dfBookingsBin = dfBookings.withColumn("booking",lit(1))

//Adding index column to searches
val dfSearchesSelIdx = dfSearches.withColumn("index",monotonically_increasing_id())


//CLEANING COLUMNS TO BE MERGED
/*
// Looking for empty values
val colsToCleanBookings = List ("arr_port","dep_port", "cre_date")
val colsToCleanSearches = List ("Origin","Destination", "Date")


// Head of nulls summary
println("NULLS REPORT")
println("")
println("SEARCHES")
println("------------")


// looping searches in search for

for (column <- colsToCleanSearches){

  val nansFound = dfSearchesSelIdx.filter(dfSearchesSelIdx(column).isNull ||
    dfSearchesSelIdx(column) === "" || dfSearchesSelIdx(column).isNaN).count()

  println(s"Total nulls in $column : $nansFound")
}

println("BOOKINGS")
println("------------")

for (column <- colsToCleanBookings){

  val nansFound = dfBookingsBin.filter(dfBookingsBin(column).isNull ||
    dfBookingsBin(column) === "" || dfBookingsBin(column).isNaN).count()

  println(s"Total nulls in $column : $nansFound")
} */

//Selecting only the necessary columns
val dfSearchesSelIdxSel = dfSearchesSelIdx.select("Origin","Destination", "Date", "index")
val dfBookingsBinSel = dfBookingsBin.select("arr_port","dep_port", "cre_date", "booking")

// Clean date on Bookings & stripping airport columns & remove duplicates
val dfBookingsBinSelRdy = dfBookingsBinSel
  .withColumn("cre_date", substring(col("cre_date"),1,10) )
  .withColumn("dep_port", trim(col("dep_port")))
  .withColumn("arr_port", trim(col("arr_port")))
  .distinct()

val dfSearchesSelIdxSelRdy = dfSearchesSelIdxSel.withColumn("Origin", trim(col("Origin")))
    .withColumn("Destination", trim(col("Destination")))


// Dropping nulls found


// Merge files Bookings with Searches on the Date, flight origin and destination
val searchesWithBookings = dfSearchesSelIdxSelRdy.join(dfBookingsBinSelRdy,
  dfSearchesSelIdxSelRdy.col("Date") === dfBookingsBinSelRdy.col("cre_date") &&
    dfSearchesSelIdxSelRdy.col("Origin") === dfBookingsBinSelRdy.col("dep_port") &&
    dfSearchesSelIdxSelRdy.col("Destination") === dfBookingsBinSelRdy.col("arr_port"), "left")

// Merge column "bookings" to the original Searches file
val searchesWithBookings_2 = searchesWithBookings.select("index","booking")



val SearchesOriginalWithBookings = dfSearchesSelIdx.join(searchesWithBookings_2,
  dfSearchesSelIdx.col("index")===searchesWithBookings_2.col("index"), "left" )

//Fill nulls in column "booking" with value 0, and remove column index

val searchesFinal = SearchesOriginalWithBookings.na.fill(0,Seq("booking"))
  .drop("index")

searchesFinal.show()

