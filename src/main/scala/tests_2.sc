
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.NumberFormat.getIntegerInstance
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import scala.util.Try

val bookingsFileTesting: String = "../dsc/Data/challenge_scala/bookings_testing.csv"


val sc = new SparkContext("local", "amadeus")
val spark = SparkSession.builder.appName("Amadeus").getOrCreate()

val df = spark.read
  .option("delimiter", "^")
  .option("header", true)
  .csv(bookingsFileTesting)

val bookingsColumns = List ("pax","dep_port","arr_port")
val bc_length = bookingsColumns.length

val aux= bookingsColumns.flatMap(c => Try(df(c)).toOption)

val aux_2 = aux.length

aux_2 == bc_length