import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import datasets.{Flight, Passenger}
import TravelAnalytics._

import scala.io.StdIn

object Main extends App {
  // prompt the user for input
  println("Enter the function to execute: \n 1. Calculate number of flights per month " +
    "\n 2. Identify frequent flyers" +
    "\n 3. Identify passengers with max travel streaks from the UK" +
    "\n 4. Identify passengers who travel together")

  // read user input
  val input = StdIn.readLine()

  implicit val spark = SparkSession.builder
    .config("spark.master", "local")
    .master("local[*]")
    .appName("Travel Analytics")
    .getOrCreate()

  import spark.implicits._

  val flightSchema = Encoders.product[Flight].schema
  val passengerSchema = Encoders.product[Passenger].schema

  val flightDf = spark.read
    .option("header", true)
    .schema(flightSchema)
    .csv("resources/flightData.csv")
    .as[Flight]

  val passengerDf = spark.read
    .option("header", true)
    .schema(passengerSchema)
    .csv("resources/passengers.csv")
    .as[Passenger]

  val outputPath: String = "resources/output/"

  // use pattern matching to call the appropriate function
  val output: DataFrame = input match {
    case "1" => numFlights(flightDf, outputPath).toDF()
    case "2" => frequentFlyer(passengerDf, flightDf, outputPath).toDF()
    case "3" => flightStreaks(flightDf, outputPath).toDF()
    case "4" => findCoTravellers(flightDf, 5, "2017-01-01", "2017-12-31", outputPath).toDF()
    case _ => {
      println("Invalid input. Please enter a valid number from 1 to 4")
      spark.emptyDataFrame
    }
  }

  // show the result if it's a DataFrame
  if (output.count() > 0) {
    output.show()
  }

  spark.stop()

}
