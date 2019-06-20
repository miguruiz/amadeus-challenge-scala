/*
* Creating test_files with random lines.
 */

// import required  classes
import org.apache.spark.SparkContext

// file_names
val searchesFile: String = "../dsc/Data/challenge/searches.csv"
val bookingsFile: String = "../dsc/Data/challenge/bookings.csv"


def createTestFile (nameFile: String, fractionSample: Double) : Unit= {
  /*
  * Takes file name and number of lines & creates testing file.
  */
  val sc = new SparkContext("local","test")
  val bookingsRdd = sc.textFile (nameFile)
  val header = bookingsRdd.first()
  val newSample = bookingsRdd.sample(false, fractionSample)

  val newName = nameFile.replace(".csv","_testing.csv")
  newSample.coalesce(1).saveAsTextFile(newName)
}

createTestFile(bookingsFile, 0.001)
//createTestFile(searchesFile, 0.001)