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
val bookingsFile: String = ""
val bookingsFileTesting: String = "../dsc/Data/challenge/bookings_testing.csv/part-00000"


// create a SparkContext object
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()

// loading data from external csv into the dataframe

val df_bookings = spark.read.format("csv").option("header","false").load(bookingsFileTesting)

df_bookings.show()

/*
* CONSIDERATIONS
*
*/

