import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import datasets.{Flight, Passenger}
import datasets.{CoTraveller, FlightStreaks, FrequentFlyer, NumFlightsPerMonth}
import TravelAnalytics._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers._

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
// Import necessary Spark libraries and case classes

class TravelAnalyticsTest extends AnyFlatSpec {

  // Create a SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("TravelAnalyticsTest")
    .master("local[2]") // Use local mode for testing
    .getOrCreate()

  import spark.implicits._

  // Define a test DataFrame of Flight data
  val testFlightData: Dataset[Flight] = Seq(
    Flight(1, 0, "in", "uk", "2023-01-01"),
    Flight(2, 0, "uk", "usa", "2023-01-01"),
    Flight(3, 0, "den", "fin", "2023-02-01"),
    Flight(4, 0, "swe", "pol", "2023-03-01"),
    Flight(5, 0, "uk", "frn", "2023-03-01")
  ).toDS()

  // Define the expected result
  val expectedResult: Dataset[NumFlightsPerMonth] = Seq(
    NumFlightsPerMonth("01", 2),
    NumFlightsPerMonth("02", 1),
    NumFlightsPerMonth("03", 2)
  ).toDS()

  //  val result = numFlights(testFlightData, "outputPath")
  //
  //  // Check if the result matches the expected result
  //  result.collect() should contain  expectedResult.collect()

  "numFlights" should "calculate the number of flights per month" in {
    // call numFlights function with test data
    val result = numFlights(testFlightData, "outputPath")

    // use ScalaTest collection matchers to check if the result matches the expected result
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  // Stop the SparkSession after all tests
  protected def afterAll(): Unit = {
    spark.stop()
  }

}
