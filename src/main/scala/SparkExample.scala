package application

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


//parallel computing for affording 100GB data  // Repartition before joining:

object SparkExample {
  def main(args: Array[String]): Unit = {

//    val existingDatasetPath = args(0)
    // val newDatasetPath = args(1)
    val newDatasetPath = "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\updatedSalary.csv"
    val s3OutputPath = "" //no path till now
    val updatedSalaryOutputPath = "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala"

    val spark = SparkSession.builder()
      .appName("SparkExample")
      //.enableHiveSupport()
      .config("spark.master", "local") //here we can specify the clusters // or we can use garbage collection which
      // manages the allocation and release of memory for an application
      .getOrCreate()
    import spark.implicits._


      def preProcessDataset(dataset: DataFrame): DataFrame = {
          val preprocessedData = dataset
            .withColumn("Value", regexp_replace($"Value", "€", ""))
            .withColumn("Value", when($"Value".endsWith("M"), regexp_replace($"Value", "M", "") * 1000000)
              .otherwise(when($"Value".endsWith("K"), regexp_replace($"Value", "K", "") * 1000)
                .otherwise($"Value")))
            .withColumn("Salary", when($"Salary".endsWith("M"), regexp_replace($"Salary", "M", "") * 1000000)
              .otherwise(when($"Salary".endsWith("K"), regexp_replace($"Salary", "K", "") * 1000)
                .otherwise($"Salary")))
            .filter($"Name".isNotNull && $"Nationality".isNotNull)
            .withColumn("Name", lower(trim($"Name")))
            .withColumn("Nationality", lower(trim($"Nationality")))
            .withColumn("Club", lower(trim($"Club")))
            .columns.foldLeft(dataset) { (accDF, colName) =>
                accDF.withColumn(colName, regexp_replace(col(colName), "\\s+", ""))
            }
            .withColumn("Value", regexp_replace($"Value", "[^\\d.]", ""))
            .withColumn("Salary", regexp_replace($"Salary", "[^\\d.]", ""))
            .withColumn("ProcessedSalary", when($"Salary".endsWith("M"), $"Salary" * 1000000)
              .when($"Salary".endsWith("K"), $"Salary" * 1000)
              .otherwise($"Salary"))

          preprocessedData
      }


      def writeOutputToCSV(data: DataFrame, outputPath: String): Unit = {
      data.coalesce(1).write
        .partitionBy("Continent")
        .mode(SaveMode.Overwrite)
        .csv(outputPath)
    }

    // Load the player dataset
    val playerData: DataFrame = spark.read
      .option("header", "true")
      .csv("C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\fifa.csv")

    // Loading the countries' continent dataset
    val countriesData: DataFrame = spark.read
      .option("header", "false")
      .csv("C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\continent.csv")
      .toDF("Continent", "Country")

    // Join player dataset with countries' continent mapping
    // TODO: Rename the dataset -- done
    // TODO: Dont exceed the line --done
    val FifaWithContinentData = playerData.join(countriesData, playerData("Nationality") === countriesData("Country"),
        "left")
      .drop("Country") // Drop the duplicate country column

    preProcessDataset(FifaWithContinentData).show(50)
    //FifaWithContinentData.show(50)


    // Save the mixed data to CSV files partitioned by continent
    FifaWithContinentData.write
      .partitionBy("Continent")
      .mode(SaveMode.Overwrite)
      .csv("output_directory")

    // Load the updated salary data from the new dataset
    val updatedSalaryData: DataFrame = spark.read
      .option("header", "true")
      .csv(newDatasetPath)

    // Join the updated salary data with the existing mixed data
    // TODO: Consider having new players in the updated dataset --done
    //Join Type is the problem
    val updatedData = FifaWithContinentData.join(updatedSalaryData, Seq("Name", "Age", "Nationality",
        "Fifa Score", "Club", "Value", "Continent"), "fullouter")
      .drop(FifaWithContinentData.col("Salary")) // Drop the current salary column
      .withColumn("Value", coalesce($"Value", lit(""))) // Handle null Value column

    updatedData.coalesce(1).write
      .partitionBy("Continent")
      .mode(SaveMode.Overwrite)
    writeOutputToCSV(updatedData, updatedSalaryOutputPath)




    // TODO: Preprocess data, add new column for processed salary, clean unwanted data --done
    // TODO: Apply this in a function --done

    //SUBSTR(Value, 2) is to extract the coin sign
    //SUBSTR(Value, -1) extracts the last character which is M or K
    //Execute the optimized Hive queries using Spark SQL API
    val topThreeCountriesQuery = {
      """
        |SELECT Nationality, SUM(CASE WHEN SUBSTR(Value, -1) = 'M'
        |THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000000
        |WHEN SUBSTR(Value, -1) = 'K' THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000
        |ELSE CAST(SUBSTR(Value, 2) AS DOUBLE) END)
        |AS TotalSalary
        |FROM mixed_data
        |GROUP BY Nationality
        |ORDER BY TotalSalary DESC
        |LIMIT 3;
        |""".stripMargin


      //TODO: FIX THIS --done "by Using Max instead of Sum"
    }
    val theMostValuablePlayerQuery =
      """
        |SELECT Club, MAX(CASE WHEN SUBSTR(Value, -1) = 'M'
        |THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000000
        |WHEN SUBSTR(Value, -1) = 'K' THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000
        |ELSE CAST(SUBSTR(Value, 2) AS DOUBLE)
        |END) AS Total
        |FROM mixed_data
        |GROUP BY Club
        |ORDER BY Total DESC
        |LIMIT 1;
        |""".stripMargin


    val topFiveSalariesClub =
      """
        |SELECT Club, SUM(CAST(SUBSTR(Salary, 2, LENGTH(Salary) - 2) AS DOUBLE)) AS TotalSalary
        |FROM mixed_data
        |GROUP BY Club
        |ORDER BY TotalSalary DESC
        |LIMIT 5;
        |""".stripMargin

    val bestFifaContinentPlayerQuery =
      """
        |SELECT CASE
        |WHEN Continent IN ('SA', 'NA') THEN 'America'
        |ELSE Continent
        |END AS CombinedContinent,
        |AVG(`Fifa Score`) AS AverageFifaScore
        |FROM mixed_data
        |WHERE Continent IN ('EU', 'SA', 'NA')
        |GROUP BY CombinedContinent
        |ORDER BY AverageFifaScore DESC
        |LIMIT 1;
        |""".stripMargin


  /*

    val bestContinentAvg =
      """
        |SELECT Continent, AVG(Fifa_Score) AS AverageFifaScore
        |FROM mixed_data
        |WHERE Continent IN ('EU', 'NA', 'SA')
        |GROUP BY Continent
        |""".stripMargin


    //re-check the continents
    val bestContinentSum =
      """
        |SELECT Continent,
        |SUM(`Fifa Score`) AS TotalFifaScore
        |FROM mixed_data
        |WHERE Continent IN ('EU', 'NA', 'SA')
        |GROUP BY Continent
        |ORDER BY TotalFifaScore DESC
        |LIMIT 1;
        |"""

*/


    val aggregatedIncomeResult = spark.sql(topThreeCountriesQuery)
    val aggregatedValueResult = spark.sql(theMostValuablePlayerQuery)
    val aggregatedSalaryResult = spark.sql(topFiveSalariesClub)
    val bestFifaContinentResult = spark.sql(bestFifaContinentPlayerQuery)
    //val averageFifaScoreResult = spark.sql(bestContinentAvg)
    //val sumFifaScoreResult = spark.sql(bestContinentSum)


    println("The Top 3 countries that achieve the highest income through their players:")
    writeOutputToCSV(aggregatedIncomeResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output")
    aggregatedIncomeResult.show()

    println("The Most Valuable Club:")
    writeOutputToCSV(aggregatedValueResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output")
    aggregatedValueResult.show()


    println("Top 5 Clubs Salary Spending:")
    writeOutputToCSV(aggregatedSalaryResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output")
    aggregatedSalaryResult.show()


    println("The Best FIFA Score by Continent:")
    writeOutputToCSV(bestFifaContinentResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output")
    bestFifaContinentResult.show()

    /*println("The Best FIFA Score by Continent Using the Average Scores Values:")
    writeOutputToCSV(averageFifaScoreResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output")
    averageFifaScoreResult.show()

    println("The Best FIFA Score by Continent Using the Sum Scores Values:")
    writeOutputToCSV(sumFifaScoreResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output")
    sumFifaScoreResult.show() */


    /*
    spark.table("aggregatedIncomeResult").show()
    spark.table("aggregatedValueResult").show()
    spark.table("aggregatedSalaryResult").show()
    spark.table("averageFifaScoreResult").show()
    spark.table("sumFifaScoreResult").show()
    */


    spark.stop()
  }
}
