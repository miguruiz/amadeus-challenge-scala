/**
  * Name: exerciseTwo.scala - top 10 arrival airports in the world in 2013
  * Description:
  * Arrival airport is the column arr_port. It is the IATA code for the airport
  *
  * To get the total number of passengers for an airport, you can sum the column pax, grouping by arr_port.
  * Note that there is negative pax. That corresponds to cancelations. So to get the total number of passengers
  * that have actually booked, you should sum including the negatives (that will remove the canceled bookings).
  *
  * Print the top 10 arrival airports in the standard output, including the number of passengers.
  * Required: Get the name of the city or airport corresponding to that airport (programmatically, we suggest to
  * have a look at OpenTravelData in Github)
  *
  */

package amadeusChallenge

// import required  classes
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, sum, trim}
import scala.util.{Try, Success, Failure}



object exerciseTwo {



/**
  * Calculates and prints the top 10 airports by passanger.
  */
  def execute (bookingFilePath: String, spark: SparkSession): DataFrame ={

    // Create Spark Session
    val dfBookings = myFunctions.readFile(bookingFilePath, spark)

    //Calculate the top airports
    val dfTopAirports = topAirports(dfBookings)


    // Print top 10
    val result = Try {myFunctions.includeAirportNames(dfTopAirports, spark)}  match {
      case Success(_) => myFunctions.includeAirportNames(dfTopAirports, spark)
      case Failure(_) => dfTopAirports
    }
    printTopN(result, 10)

    //Return result - used in the web service
    result
  }

/**
  * Given bookings.csv, retunrs a dataframe with airports sorted by number of passangers
  */
  def topAirports (dfBookings: DataFrame): DataFrame ={

    //Clean column names
    val dfBookingsClean =  myFunctions.cleanColumnNames(dfBookings)

    // Selecting act_date and arr_port
    val dfSel =dfBookingsClean.select("act_date", "arr_port", "pax")

    // Filter year 2013
    val dfSel2013 = dfSel.filter(dfSel("act_date").contains("2013"))

    //Strip column "arr_port" - for merging with OpenDataTravel
    val defSel2013Clean = dfSel2013.withColumn("arr_port", trim(col("arr_port")))

    // Group by airport adding passangers and sort by num of pax
    defSel2013Clean.groupBy("arr_port")
      .agg(sum("pax").alias("pax_sum"))
      .sort(desc("pax_sum"))
  }

/**
  * Prints the n top airports
  */
  def printTopN (df: DataFrame, n: Int): Unit ={
    df.show(n)
  }




}
