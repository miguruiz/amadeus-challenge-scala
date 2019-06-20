/*
 * EXERCISE TWO: TOP 10 ARRIVAL AIRPORTS IN 2013
 * Details:
 *  - use bookings.csv
 *  - use arr_port (IATA code for the airport)
 *  - sum the column pax, grouping by arr_port. Note that there is negative pax (cancelations)
 *  - Print the top 10 arrival airports in the standard output, including the number of passengers.
 *  - Get the name of the city or airport corresponding to that airport ( OpenTravelData)
 */

// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// file_names
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"
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

val colNames = Seq("act_date", "arr_port")

dfRenamed


spark.close()
/*
* CONSIDERATIONS
*
*/

