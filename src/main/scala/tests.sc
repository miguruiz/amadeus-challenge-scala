
import org.apache.spark.SparkContext

// file_names
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"
val bookingsFileTesting: String = "../dsc/Data/challenge/bookings_testing.csv/part-00000"

val sc = new SparkContext("local","test")
val bookingsRdd = sc.textFile (bookingsFile)
val header_1 = bookingsRdd.first()
