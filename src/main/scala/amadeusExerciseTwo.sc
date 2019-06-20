/*
 * EXERCISE TWO: TOP 10 ARRIVAL AIRPORTS IN 2013
 * Details:
 *  - use bookings.csv
 *  - use arr_port (IATA code for the airport)
 *  - sum the column pax, grouping by arr_port. Note that there is negative pax (cancelations)
 *  - Print the top 10 arrival airports in the standard output, including the number of passengers.
 *  - Get the name of the city or airport corresponding to that airport ( OpenTravelData)
 */

//PART I: Select top 10 arrival airports

// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, desc}

// file_names
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"
val bookingsFileUnique: String = "../dsc/Data/challenge_scala/bookings_unique.csv/part-00000"
val bookingsFileTesting: String = "../dsc/Data/challenge/bookings_testing.csv"

val fileInUse = bookingsFileTesting

// create a SparkContext object
val sc = new SparkContext("local","amadeus-challenge")

// Create Spark Session
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()

// Import file to dataframe setting header and delimiter

val df = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUse)

// Show first 20 lines
df.columns
df.show()
df.printSchema()


//Clean column names
val newColumnNames = df.columns.map(_.replace (" ",""))

//Creating new dataframe with cleaned names
val dfRenamed = df.toDF(newColumnNames: _*)

// Selecting act_date and arr_port
val columnName = Seq("act_date", "arr_port", "pax")
var dfArrivals =dfRenamed.select(columnName.head, columnName.tail: _*)
dfArrivals.count()

// Filter year 2013
val dfArrivals2013 = dfArrivals.filter(dfArrivals("act_date").contains("2013"))

//Check for nulls in "pax" column
val paxNans = dfArrivals2013.filter(dfArrivals2013("pax").isNull ||
  dfArrivals2013("pax") === "" ||
  dfArrivals2013("pax").isNaN).count()

println(s"nulls in Pax: $paxNans")

//Check for nulls in "arr_port" column
val arrNans = dfArrivals2013.filter(dfArrivals2013("pax").isNull ||
  dfArrivals2013("arr_port") === "" ||
  dfArrivals2013("arr_port").isNaN).count()
println(s"nulls in arr_port: $arrNans")

//No nulls detected in arr_port and pax (correct?)

// Agregate by "arr_port"
val topAirports = dfArrivals2013.groupBy("arr_port")
  .agg(sum("pax").alias("pax_sum"))
  .sort(desc("pax_sum"))

//Display top 10
topAirports.take(10)

//PART II: merge with OpenTravelData to get airport names
val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"


spark.close()

/*
* CONSIDERATIONS
* Since my experience with Scala spark is more limited than my experience with Pandas, workflow is extremly important.
* Working only with the necessary data, removing duplicates, checking for nulls, data types,etc.
*/

