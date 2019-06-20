
//From: https://github.com/xavierguihot/geobase
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split }



//PART II: merge with OpenTravelData to get airport names
val sc = new SparkContext("local","amadeus-challenge")
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()
import spark.implicits._


val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"


val content = scala.io.Source.fromURL(url).mkString
val list = content.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)




// note the necessary escaping because | is a special character in regular expressions
val arrays = rdd.map(_.split("\\^"))

// otherwise - can use first record to determine number of columns:
val maxCols = arrays.first().length

// now we create a column per (1 .. maxCols) and select these:
val result = arrays.toDF("arr")
  .select((0 until maxCols).map(i => $"arr"(i).as(s"col_$i")): _*)

result.se
result.show()
