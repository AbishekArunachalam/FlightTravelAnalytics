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

2. To process data, use the following command:

  ```sh
  sbt "runMain src.example.TravelAnalytics path/input_file.csv path/output_file.csv"

