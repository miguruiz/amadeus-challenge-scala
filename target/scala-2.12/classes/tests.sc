
//From: https://github.com/xavierguihot/geobase
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split }



//PART II: merge with OpenTravelData to get airport names
val sc = new SparkContext("local","amadeus-challenge")
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()
import spark.implicits._

//URL from OpenTravelData - GeoBases
val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"

//Parse URL to RDD
val content = scala.io.Source.fromURL(url).mkString
val list = content.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)

//Split into Columns
val arrays = rdd.map(_.split("\\^"))

// Calculate total number of clumns
val maxCols = arrays.first().length

// Converting RDD to Datafrae and giving general names to columns
val result = arrays.toDF("arr")
  .select((0 until maxCols).map(i => $"arr"(i).as(s"col_$i")): _*)

// Selecting columns IATA code and Airport Name
val resultSelection = result.select("col_0","col_1").show()


