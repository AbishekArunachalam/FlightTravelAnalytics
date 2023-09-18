import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class FrequentFlyerTest extends AnyFunSuite {

  // Create a SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("FrequentFlyerTest")
    .master("local[2]") // Use local mode for testing
    .getOrCreate()

  import spark.implicits._

  // Sample data
  val passengers: Dataset[Passenger] = Seq(
    Passenger(1, "John", "Doe"),
    Passenger(2, "Jane", "Smith"),
    Passenger(3, "Alice", "Johnson")
  ).toDS()

  val flights: Dataset[Flight] = Seq(
    Flight(1, "ABC123"),
    Flight(1, "XYZ789"),
    Flight(2, "DEF456"),
    Flight(3, "GHI789"),
    Flight(3, "JKL123")
  ).toDS()

  test("frequentFlyer should return the DataFrame with frequent flyers") {
    val result: DataFrame = FrequentFlyer.frequentFlyer(passengers, flights)

    // Check if the result contains the expected columns
    assert(result.columns === Array("passengerId", "numFlights", "firstName", "lastName"))

    // Check if the result contains the expected number of rows
    assert(result.count() === 3)

    // Check if the result is sorted correctly by numFlights in descending order
    val numFlightsColumn = result.select("numFlights").collect().map(_.getInt(0))
    assert(numFlightsColumn === Array(2, 2, 1))
  }

  // Stop the SparkSession after all tests are executed
  after {
    spark.stop()
  }
}

// Define case classes for Passenger and Flight
case class Passenger(passengerId: Int, firstName: String, lastName: String)
case class Flight(passengerId: Int, flightNumber: String)