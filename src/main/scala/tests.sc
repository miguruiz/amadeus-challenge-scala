
//From: https://github.com/xavierguihot/geobase
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


//PART II: merge with OpenTravelData to get airport names
val sc = new SparkContext("local","amadeus-challenge")
val spark = SparkSession.builder.appName("Amadeus Exercise Two Application").getOrCreate()
import spark.implicits._


val url: String ="https://raw.githubusercontent.com/opentraveldata/geobases/public/GeoBases/DataSources/Airports/GeoNames/airports_geonames_only_clean.csv"

val content = scala.io.Source.fromURL(url).mkString
val list = content.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)
val df2 = rdd.toDF()




df2.show()

