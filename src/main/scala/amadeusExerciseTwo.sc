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
import org.apache.spark.sql.functions.{sum, desc, trim, col }


// file_names
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"
val bookingsFileUnique: String = "../dsc/Data/challenge_scala/bookings_unique.csv/part-00000-1064b8ed-2021-4023-81fb-3f22d2d0608b-c000.csv"
val bookingsFileTesting: String = "../dsc/Data/challenge_scala/bookings_testing.csv"

val fileInUse = bookingsFileUnique

// create a SparkContext object
val sc = new SparkContext("local","amadeus-challenge")

// Create Spark Session
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()
import spark.implicits._

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
var dfArrivals =dfRenamed.select("act_date", "arr_port", "pax")

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

//Strip column "arr_port"

val dfArrivals2013Clean = dfArrivals2013
  .withColumn("arr_port", trim(col("arr_port")))


dfArrivals2013Clean.columns

// Agregate by "arr_port"
val topAirports = dfArrivals2013Clean.groupBy("arr_port")
  .agg(sum("pax").alias("pax_sum"))
  .sort(desc("pax_sum"))

//Display top 10
topAirports.show(10)

//PART II: merge with OpenTravelData to get airport names

//URL from OpenTravelData - GeoBases
val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"

//Parse URL to RDD
val geoContent = scala.io.Source.fromURL(url).mkString
val geoContentList = geoContent.split("\n").filter(_ != "")
val geoContentRdd = sc.parallelize(geoContentList)

//Split into Columns
val geoContentRddArrays = geoContentRdd.map(_.split("\\^"))

// Calculate total number of clumns
val maxCols = geoContentRddArrays.first().length

val newColNames = Seq("IATA_code", "Airport_name")

// Converting RDD to Datafrae and giving general names to columns
val geoContentDf = geoContentRddArrays.toDF("arr")
  .select((0 until maxCols).map(i => $"arr"(i).as(s"col_$i")): _*)

// Selecting columns IATA code and Airport Name
val resultSelection = geoContentDf.select("col_0","col_1").toDF(newColNames: _*)

//Joining top 10 airports dataframe with GeoBases information

val topAirportsNames = topAirports.join(resultSelection,
  topAirports.col("arr_port") === resultSelection.col("IATA_code"))

topAirportsNames.sort(desc("pax_sum")).show(10)

spark.close()

/*
* CONSIDERATIONS
* Since my experience with Scala spark is more limited than my experience with Pandas, workflow is extremly important.
* Working only with the necessary data, removing duplicates, checking for nulls, data types,etc.
*/

