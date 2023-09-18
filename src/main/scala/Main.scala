import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Encoders
import datasets.{Flight, Passenger}
import TravelAnalytics._

object Main extends App {

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

  val numOfFlights = numFlights(flightDf)
  numOfFlights.coalesce(1).write
    .option("header", "true")
    .csv("resources/output/problem1.csv")

  val freqFlyerDf = frequentFlyer(passengerDf, flightDf)
  freqFlyerDf.coalesce(1).write
    .option("header", "true")
    .csv("resources/output/problem2.csv")

  val flightStreak = flightStreaks(flightDf)
  flightStreak.coalesce(1).write
    .option("header", "true")
    .csv("resources/output/problem3.csv")

  val numCoTravels = findCoTravellers(flightDf, 5, "2017-01-01", "2017-12-31")
  numCoTravels.coalesce(1).write
    .option("header", "true")
    .csv("resources/output/problem4.csv")

  spark.stop()

}
