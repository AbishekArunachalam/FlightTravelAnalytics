# FlightTravelAnalytics
A Scala based Apache-Spark project for data processing and analysis

## Table of Contents

- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Usage](#usage)

## Prerequisites

Before you begin, ensure you have met the following requirements:

- [Apache Spark](https://spark.apache.org/) version 2.4.8 installed and configured.
- [Scala](https://www.scala-lang.org/) version 2.12.10 installed.
- [SBT](https://www.scala-sbt.org/) build tool.


## Getting Started

1. Clone this repository:

   ```sh
   git clone https://github.com/AbishekArunachalam/FlightTravelAnalytics.git
   
   cd scala-spark-project
   sbt clean compile
   
   sbt run

## Usage

The scala program consists of the following functions:

1. **numFlights(flightsDs: Dataset[Flight], outputPath: String)** - To calculate the number of flights per month

2. **frequentFlyer(passengerDs: Dataset[Passenger], flightsDs: Dataset[Flight], outputPath: String)** - To identify the frequent flyers

3. **flightStreaks(flightsDs: Dataset[Flight], outputPath: String)** - To identify the passengers with max travel streaks from the UK

4. **findCoTravellers(flightsDs: Dataset[Flight], atleastNTimes: Int, from: String, to: String, outputPath: String)** - To identify passenger who travel together

To run the program:
  ```sh
  sbt "runMain src.main.scala.Main"

Enter the function number to execute when prompted. **findCoTravellers** gets user input before transforming the data.

