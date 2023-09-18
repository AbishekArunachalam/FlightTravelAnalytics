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
  ```

## Sample Output

Enter the function to execute: 4

**findCoTravellers** gets user input before transforming the data.

Enter the minimum number of co-travellers:
5
Enter the start date (e.g., 2017-01-01):
2017-01-01
Enter the end date (e.g., 2017-12-31):
2017-12-30

+-----------+------------+------------------+
|passengerId|passengerId2|numFlightsTogether|
+-----------+------------+------------------+
|        760|         701|                15|
|       2759|        2717|                14|
|       3590|        3503|                14|
|       5490|        2939|                13|
|       4373|        4316|                12|
|       3093|        1208|                12|
|        392|         382|                12|
|       4316|        2759|                12|
|       1484|        1337|                12|
+-----------+------------+------------------+