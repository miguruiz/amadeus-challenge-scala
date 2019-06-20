// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


// file_names
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"
val bookingsFileUnique: String = "../dsc/Data/challenge_scala/bookings_unique.csv"
val bookingsFileTesting: String = "../dsc/Data/challenge_scala/bookings_testing.csv"

val searchesFile: String = "../dsc/Data/challenge_scala/searches.csv"
val searchesFileUnique: String = "../dsc/Data/challenge_scala/searches_unique.csv"
val searchesFileTesting: String = "../dsc/Data/challenge_scala/searches_testing.csv"


val fileInUseBookings = bookingsFile
val fileInUseSearches = searchesFile

// create a SparkContext object
val sc = new SparkContext("local","amadeus-challenge")

// Create Spark Session
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()

// Import file to dataframe setting header and delimiter

val dfBookings = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUseBookings)

val dfSearches = spark.read
  .option("delimiter", "^")
  .option ("header","true")
  .csv(fileInUseSearches)


dfBookings
  .distinct()
  .coalesce(1)
  .write.format("csv")
  .option("header", "true")
  .option("delimiter", "^")
  .save(bookingsFileUnique)

dfSearches
  .distinct()
  .coalesce(1)
  .write.format("csv")
  .option("header", "true")
  .option("delimiter", "^")
  .save(searchesFileUnique)