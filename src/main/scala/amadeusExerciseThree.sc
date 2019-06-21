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
 */

//PART I: Select top 10 arrival airports

// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, desc, trim, col, lit,monotonically_increasing_id }


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

val dfBookings = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUseBookings)

val dfSearches = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUseSearches)


//Clean column names in both dataframes
val newColumnNamesSearches = dfSearches.columns.map(_.replace (" ",""))
val newColumnNamesBookings = dfBookings.columns.map(_.replace (" ",""))

//Creating new dataframe with cleaned column names
val dfSearchesCleaned  = dfSearches.toDF(newColumnNamesSearches: _*)
val dfBookingsCleaned  = dfBookings.toDF(newColumnNamesBookings: _*)

//Adding index column to searches
dfSearchesCleaned.withColumn("index",monotonically_increasing_id())

//Adding value 1 to bookings
dfBookingsCleaned.withColumn("booking",lit(1))


dfSearchesCleaned.show()
dfBookingsCleaned.show()
//CLEANING COLUMNS TO BE MERGED

// Looking for empty values
val colsToCleanSearches = List ("Origin","Destination", "Date")
val colsToCleanBookings = List ("arr_port","dep_port", "cre_date")

// Head of nulls summary
println("NULLS REPORT")
println("")
println("SEARCHES")
println("------------")

// looping searches in search for
for (column <- colsToCleanSearches){

  val nansFound = dfSearchesCleaned.filter(dfSearchesCleaned(column).isNull ||
    dfSearchesCleaned(column) === "" || dfSearchesCleaned(column).isNaN).count()

  println(s"Total nulls in $column : $nansFound")
}

println("BOOKINGS")
println("------------")

for (column <- colsToCleanBookings){

  val nansFound = dfBookingsCleaned.filter(dfBookingsCleaned(column).isNull ||
    dfBookingsCleaned(column) === "" || dfBookingsCleaned(column).isNaN).count()

  println(s"Total nulls in $column : $nansFound")
}

//Selecting only the necessary columns
val dfSearchesCleanedSel = dfSearchesCleaned.select("Origin","Destination", "Date", "index")
val dfBookingsCleanedSel = dfBookingsCleaned.select("arr_port","dep_port", "cre_date", "booking")



// Dropping nulls found
