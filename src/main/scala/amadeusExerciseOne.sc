/*
 * EXERCISE ONE: COUNT LINES
 */

// import required  classes
import org.apache.spark.SparkContext

// file_names
val searchesFile: String = "../dsc/Data/challenge_scala/searches.csv"
val bookingsFile: String = "../dsc/Data/challenge_scala/bookings.csv"

val searchesFileTesting: String = "../dsc/Data/challenge_scala/searches_testing.csv"
val bookingsFileTesting: String = "../dsc/Data/challenge_scala/bookings_testing.csv"

// Files to use
val bookiesUsedFile = searchesFileTesting
val searchesUsedFile = bookingsFileTesting

// create a SparkContext object
val sc = new SparkContext("local","amadeus-challenge")

// loading data from external dataset
val searchesRdd = sc.textFile(bookiesUsedFile)
val bookingsRdd = sc.textFile(searchesUsedFile)

//Calculate total lines in searches & bookings files
val linesSearches = searchesRdd.count()
val linesBookings = bookingsRdd.count()

//Counting unique lines
val uniqueLinesSearches = searchesRdd.distinct().count()
val uniqueLinesBookings = bookingsRdd.distinct().count()

// Adding "," to the thousands in the number of lines
val formatter = java.text.NumberFormat.getIntegerInstance
val linesSearchesFormatted = formatter.format(linesSearches)
val linesBookingsFormatted = formatter.format(linesBookings)
val UniqueLinesSearchesFormatted = formatter.format(uniqueLinesSearches)
val UniqueLinesBookingsFormatted = formatter.format(uniqueLinesBookings)

//Printing results
println("SPARK RESULTS")
println("")
println("   Searches.csv")
println("   ------------")
println(s"   Total lines : $linesSearchesFormatted")
println(s"   Unique lines:  $UniqueLinesSearchesFormatted")
println("")
println("   Bookings.csv")
println("   ------------")
println(s"   Total lines : $linesBookingsFormatted")
println(s"   Unique lines:  $UniqueLinesBookingsFormatted")

//Stopping SparkContet
sc.stop()
println("**SPARK STOPPED. END COUNTING LINES**")

/*
* CONSIDERATIONS
* - The reason why I check for duplicates now, is because in the Python challenge
*   there were duplicates, hence is worth cleaning the files now.
* - If Spark was not allowed:
*   - Lines could be counted using:"scala.io.Source.fromFile("FileName").getLines.size"
*   - Duplicates could be removed using the following indications:
*      https://blog.cyberwhale.tech/2017/01/09/remove-duplicate-lines-from-file-in-scala/
*
* - Would caching help?
*/