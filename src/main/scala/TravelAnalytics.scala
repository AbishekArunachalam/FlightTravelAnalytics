import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import datasets.{Flight, Passenger}
import datasets.{CoTraveller, FlightStreaks, FrequentFlyer, NumFlightsPerMonth}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import Main.{flightDf, spark}

object TravelAnalytics {

  import spark.implicits._

  def numFlights(flightsDs: Dataset[Flight]): Dataset[NumFlightsPerMonth] = {
    // create a new column month from date
    val flightsMnth = flightsDs.withColumn("month",
      date_format(to_date(col("date"), "yyyy-MM-dd"), "MM"))

    // group by month and calculate number of flights per month
    val numFlightsMnth = flightsMnth
      .groupBy(col("month"))
      .agg(count("*").alias("numFlights"))
      .orderBy(col("month"))

    // return dataset
    return numFlightsMnth.as[NumFlightsPerMonth]
  }

  def frequentFlyer(passengerDs: Dataset[Passenger], flightsDs: Dataset[Flight]): Dataset[FrequentFlyer] = {

    // number of flight the passenger has taken
    val flightsTaken = flightsDs.groupBy(col("passengerId"))
      .agg(count("*").alias("numFlights"))

    // join flights taken with passenger dataframe
    val travelDf = flightsTaken.join(passengerDs,
      Seq("passengerId"), "inner")

    // get the first name and last name of passenger
    val freqFlyer = travelDf.select("passengerId", "numFlights", "firstName", "lastName")
      .withColumn("numFlights", col("numFlights").cast(IntegerType))
      .orderBy(desc("numFlights"))

    // return dataset
    return freqFlyer.as[FrequentFlyer]
  }

  def flightStreaks(flightsDs: Dataset[Flight]): Dataset[FlightStreaks] = {
    // declare window specs
    val rowNumWindow = Window
      .partitionBy("passengerId")
      .orderBy("date")
    val returnRowNumWindow = Window
      .partitionBy("passengerId")
      .orderBy("date")
      .rangeBetween(Window.currentRow, Window.unboundedFollowing)

    val backAndForth = flightsDs
      // create a row number window function partition by passengerId
      .withColumn("rowNum", row_number().over(rowNumWindow))
      // create a window function to populate values based on return to the UK
      .withColumn("ukReturnRowNum", min(when(col("to") === "uk", $"rowNum")
        .otherwise(null)).over(returnRowNumWindow))
      // filter rows with passenger departing UK
      .where(col("from") === "uk")
      .withColumn("maxTravelStreak", col("ukReturnRowNum") - col("rowNum"))

    // aggregate and get the max travel streak returning to UK for each passenger
    val maxTravelStreaks = backAndForth.groupBy("passengerId")
      .agg(coalesce(max(col("maxTravelStreak")), lit(0)).as("longestRun"))
      .orderBy(desc("longestRun"))

    // return a dataset
    return maxTravelStreaks.as[FlightStreaks]
  }

  def findCoTravellers(flightsDs: Dataset[Flight], atleastNTimes: Int, from: String, to: String): Dataset[CoTraveller] = {
    // filter data based on timelines provided as arguments
    val flightDf2 = flightsDs.withColumnRenamed("passengerId", "passengerId2")
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .filter(col("date") >= lit(from) && col("date") <= lit(to))

    // perform self join to identify passengers travelling in same flight
    val selfJoinDF = flightsDs.join(flightDf2, Seq("flightId", "date"), "inner")
      .where(col("passengerId") =!= col("passengerId2"))
      .orderBy(desc("passengerId"))

    // filter data based on number of time passenger has flown together
    val numCoTravels = selfJoinDF.groupBy(col("passengerId"), col("passengerId2"))
      .agg(countDistinct("flightId").alias("numFlightsTogether"))
      .filter(col("numFlightsTogether") >= atleastNTimes)

    // remove duplicated records by sorting array of passenger ids
    val distinctNumCoTravels = numCoTravels.withColumn("passengerIdArr",
      sort_array(array(col("passengerId"), col("passengerId2"))))
      .dropDuplicates(Seq("passengerIdArr"))
      .drop(col("passengerIdArr"))
      .withColumn("numFlightsTogether", col("numFlightsTogether").cast(IntegerType))
      .orderBy(desc("numFlightsTogether"))

    // return dataset
    return distinctNumCoTravels.as[CoTraveller]
  }
}
