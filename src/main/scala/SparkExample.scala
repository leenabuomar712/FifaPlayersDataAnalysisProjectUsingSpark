package application

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkExample {
  def main(args: Array[String]): Unit = {

    val newDatasetPath = "C:\\Users\\Hp\\IdeaProjects\\Example\\allData.csv\\part-00000-48c81e78-45e0-4385-b441-19d06a5214e1-c000.csv"
    val updatedSalaryOutputPath = "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\output_result"

    val spark = SparkSession.builder()
      .appName("SparkExample")
      .config("spark.master", "local")
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

      val columnsToClean = preprocessedData.columns
      val cleanedData = columnsToClean.foldLeft(preprocessedData) { (accDF, colName) =>
        accDF.withColumn(colName, regexp_replace(col(colName), "\\s+", ""))
      }

      val finalProcessedData = cleanedData
        .withColumn("Value", regexp_replace($"Value", "[^\\d.]", ""))
        .withColumn("Salary", regexp_replace($"Salary", "[^\\d.]", ""))
        .withColumn("ProcessedSalary", when($"Salary".endsWith("M"), regexp_replace($"Salary", "M", "") * 1000000)
          .when($"Salary".endsWith("K"), regexp_replace($"Salary", "K", "") * 1000)
          .otherwise($"Salary"))

      preprocessedData.show()
      cleanedData.show()
      finalProcessedData.show()

      finalProcessedData
    }
    
    // Function to write DataFrame to CSV
    def writeResultToCSV(result: DataFrame, outputPath: String): Unit = {
      result.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputPath)
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

    //preProcessDataset(FifaWithContinentData).show(50)

    // Save the mixed data to CSV files partitioned by continent
    FifaWithContinentData.write
      .partitionBy("Continent")
      .mode(SaveMode.Overwrite)
      .csv("output_directory")

    // Load the updated salary data from the new dataset
    val updatedSalaryData: DataFrame = spark.read
      .option("header", "true")
      .csv(newDatasetPath)


    // Join the updated salary data with the existing entire data
    // TODO: Consider having new players in the updated dataset --done
    val updatedData = FifaWithContinentData.join(updatedSalaryData, Seq("Name", "Age", "Nationality",
        "Fifa Score", "Club", "Value", "Continent"), "fullouter")
      .drop(FifaWithContinentData.col("Salary")) // Drop the current salary column
      .withColumn("Value", coalesce($"Value", lit(""))) // Handle null Value column

    updatedData.coalesce(1).write
      .partitionBy("Continent")
      .mode(SaveMode.Overwrite)
    writeResultToCSV(updatedData, updatedSalaryOutputPath)

    // TODO: Preprocess data, add new column for processed salary, clean unwanted data --done
    // TODO: Apply this in a function --done
    preProcessDataset(FifaWithContinentData).show(50)

    FifaWithContinentData.createOrReplaceTempView("FifaContinentData")

    //SUBSTR(Value, 2) is to extract the coin sign
    //SUBSTR(Value, -1) extracts the last character which is M or K
    val topThreeCountriesQuery = {
      """
        |SELECT Nationality, SUM(CASE WHEN SUBSTR(Value, -1) = 'M'
        |THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000000
        |WHEN SUBSTR(Value, -1) = 'K' THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000
        |ELSE CAST(SUBSTR(Value, 2) AS DOUBLE) END)
        |AS TotalSalary
        |FROM FifaContinentData
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
        |FROM FifaContinentData
        |GROUP BY Club
        |ORDER BY Total DESC
        |LIMIT 1;
        |""".stripMargin


    val topFiveSalariesClub =
      """
        |SELECT Club, SUM(CASE WHEN SUBSTR(Value, -1) = 'M'
        |THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000000
        |WHEN SUBSTR(Value, -1) = 'K' THEN CAST(SUBSTR(Value, 2, LENGTH(Value) - 2) AS DOUBLE) * 1000
        |ELSE CAST(SUBSTR(Value, 2) AS DOUBLE)
        |END) AS TotalSalary
        |FROM FifaContinentData
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
        |FROM FifaContinentData
        |WHERE Continent IN ('EU', 'SA', 'NA')
        |GROUP BY CombinedContinent
        |ORDER BY AverageFifaScore DESC
        |LIMIT 1;
        |""".stripMargin

    val aggregatedIncomeResult = spark.sql(topThreeCountriesQuery)
    val aggregatedValueResult = spark.sql(theMostValuablePlayerQuery)
    val aggregatedSalaryResult = spark.sql(topFiveSalariesClub)
    val bestFifaContinentResult = spark.sql(bestFifaContinentPlayerQuery)

    println("The Top 3 countries that achieve the highest income through their players:")
    writeResultToCSV(aggregatedIncomeResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\Query1_Output")
    aggregatedIncomeResult.show()

    println("The Most Valuable Club:")
    writeResultToCSV(aggregatedValueResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\Query2_output")
    aggregatedValueResult.show()
    
    println("Top 5 Clubs Salary Spending:")
    writeResultToCSV(aggregatedSalaryResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\Query3_Output")
    aggregatedSalaryResult.show()
    
    println("The Best FIFA Score by Continent:")
    writeResultToCSV(bestFifaContinentResult, "C:\\Users\\Hp\\IdeaProjects\\Example\\src\\main\\scala\\Query4_Output")
    bestFifaContinentResult.show()


    spark.stop()
  }
}
