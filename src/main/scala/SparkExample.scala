package application

import utils.Constants._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkExample {

  /**
   * Read data from a CSV file and return it as a DataFrame.
   *
   * @param spark       The SparkSession to be used for DataFrame operations.
   * @param filePath    The path to the CSV file.
   * @param hasHeader   Indicates whether the CSV file has a header row (default: true).
   * @param columnNames Optional column names to be used if the CSV file does not have a header.
   * @return A DataFrame containing the data from the CSV file.
   */
  def readFromCSV(spark: SparkSession, filePath: String, hasHeader: Boolean = true, columnNames: Seq[String] = Seq.empty): DataFrame = {
    val reader = spark.read.option("header", hasHeader.toString)
    val csvData = if (hasHeader) {
      reader.csv(filePath)
    } else {
      val dataWithHeader = reader.option("inferSchema", "true").csv(filePath)
      if (columnNames.nonEmpty) {
        dataWithHeader.toDF(columnNames: _*)
      } else {
        dataWithHeader
      }
    }
    csvData
  }

  /**
   * Write the given DataFrame to CSV files in the specified output directory.
   *
   * @param result The DataFrame to be written to CSV.
   * @param outputPath The directory where CSV files will be saved.
   */
  def writeResultToCSV(result: DataFrame, outputPath: String): Unit = {
    result.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputPath)
  }

  /**
   * Write the given DataFrame to CSV files with a specified partition column.
   *
   * @param result The DataFrame to be written to CSV.
   * @param outputPath  The directory where CSV files will be saved.
   * @param partitionColumn The column based on which the data will be partitioned.
   */
  def writeResultToCSVwithColumn(result: DataFrame, outputPath: String, partitionColumn: String): Unit = {
    result.coalesce(1).write
      .partitionBy(partitionColumn)
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)
  }

  /**
   *
   * Preprocesses the input DataFrame by performing various data cleansing and transformation operations.
   *
   * This function takes a SparkSession and a DataFrame as input and applies the following transformations:
   * - Cleanses the 'Value', 'Continent', and 'Salary' columns by removing non-alphanumeric characters.
   * - Converts currency values ('Value' and 'Salary') to numeric values, handling 'M' (million) and 'K' (thousand) suffixes.
   * - Filters out rows where 'Name' or 'Nationality' columns have null values.
   * - Converts 'Name', 'Nationality', and 'Club' columns to lowercase and removes leading/trailing spaces.
   * - Removes extra whitespace from all columns.
   *
   * @param spark   The SparkSession instance.
   * @param dataset The input DataFrame to be preprocessed.
   * @return A new DataFrame with the preprocessed data.
   *
   */
  def preProcessDataset(spark: SparkSession, dataset: DataFrame): DataFrame = {
    import spark.implicits._
    val preprocessedData = dataset
      .withColumn("Value", regexp_replace($"Value", "[^a-zA-Z0-9]", ""))
      .withColumn("Continent", regexp_replace($"Continent", "'", ""))
      .withColumn("Salary", regexp_replace($"Salary", "[^a-zA-Z0-9]", ""))
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

    preprocessedData.show()
    cleanedData
  }

  def main(args: Array[String]): Unit = {

    // TODO: Create a new package, called utils.Constants
    /**
    * The utils.Constants is a header file contains all the user-defined constants
     */
    val newDatasetPath = utils.Constants.newDatasetPath
    val updatedSalaryOutputPath = utils.Constants.updatedSalaryOutputPath

    val spark = SparkSession.builder()
      .appName("SparkExample")
      .config("spark.master", "local")
      .getOrCreate()

    // TODO: Use caching in an efficient way
    // TODO, Export preProcessDataset as a new function
    // TODO, Preprocess the continent column
    // TODO: Document your function
    // TODO: EXPORT writeCSVOutput AS A NEW FUNCTION
    // todo: do functions documentation

    /**
     * Load CSV data with header
     */
    val playerData: DataFrame = readFromCSV(spark, utils.Constants.fifaPlayersDataset, hasHeader = true)

    /**
     * Load CSV data without header
     */
    val countriesData: DataFrame = readFromCSV(spark, utils.Constants.continentDataset,
      hasHeader = false, columnNames = Seq("Continent", "Country"))

    /**
    * Join player dataset with countries' continent mapping
     */
    val FifaWithContinentData = playerData.join(countriesData, playerData("Nationality") === countriesData("Country"),
        "left")

    /**
    * Drop the duplicate country column
    */
      // TODO: DOCUMENTATION BEFORE THE LINE, LEAVE A BLANK LINE BEFORE THE COMMENT
      .drop("Country")

    /**
     * Caches the DataFrame 'FifaWithContinentData' into memory to optimize performance,
     * in order to allow for faster access when used in subsequent operations.
     * applying caching here is because the DataFrame is accessed multiple times and utilized in iterative computations.
     */
    FifaWithContinentData.cache()

    /**
    * Save the FIFA Players data  data to CSV files partitioned by continent
     */
    // TODO: USE writeToCsv function
    writeResultToCSVwithColumn(FifaWithContinentData, "output_directory", "Continent")

    // todo: export new read function and use
    // todo: pass the new dataset

    val updatedSalaryData: DataFrame = readFromCSV(spark, newDatasetPath, hasHeader = true)
    val aliasedUpdatedSalaryData = updatedSalaryData
      .withColumnRenamed("Salary", "UpdatedSalary")

    // TODO: Consider having new players in the updated dataset
    // TODO, FULL OUTER IS GREAT CHOICE, BUT USE IT RIGHT

    /**
    * Join the updated salary data with the existing entire data
     */
    val updatedData = FifaWithContinentData
      .join(aliasedUpdatedSalaryData, Seq("Name", "Age", "Nationality", "Fifa Score", "Club", "Value", "Continent"),
        "fullouter")

    /**
    * Use coalesce to merge values based on conditions,
     * we should keep in mind the updating for the salary values may cause null values,
     * to deal with this issue, the 'when' condition was applied 
     * the salary is updated only if there is a non-null value in the "UpdatedSalary" column.
     * If "UpdatedSalary" is null, we'll keep the existing salary value
     */
    val mergedData = updatedData
      .withColumn("Value", coalesce(aliasedUpdatedSalaryData("Value"), updatedData("Value")))
      .withColumn("Salary", coalesce(
        when(aliasedUpdatedSalaryData("UpdatedSalary").isNotNull, aliasedUpdatedSalaryData("UpdatedSalary"))
          .otherwise(updatedData("Salary"))))

    mergedData.show()
    writeResultToCSV(mergedData, updatedSalaryOutputPath)

    /*
    this is the old JOIN statement
    val updatedData = FifaWithContinentData.join(updatedSalaryData, Seq("Name", "Age", "Nationality",
        "Fifa Score", "Club", "Value", "Continent"), "fullouter")
      // TODO: REMOVE, AND FIX THE COMMENT STYLING
      .drop(FifaWithContinentData.col("Salary"))
      // Drop the current salary column
      // TODO: DROP AFTER USING PREPROCESSING
      .withColumn("Value", coalesce($"Value", lit(""))) // Handle null Value column

    */


    // TODO: PREPROCESS BEFORE PROCEEDING WITH EXTRACTING RESULTS, DROP SHOW
    preProcessDataset(spark, mergedData).createOrReplaceTempView("FifaContinentData")

    // todo: fix the documentation
    // todo: No need for preprocessing


    /**
     * Retrieve the top three countries with the highest total player salaries.
     * This query calculates the total salary for each nationality in the dataset and
     * returns the top three countries with the highest total player salaries.
     *
     * @return A DataFrame containing the top three countries and their total player salaries,
     * ordered by the total salary in descending order.
     */
    val topThreeCountriesQuery = {
      """
        |SELECT Nationality, SUM(Value)
        |AS TotalSalary
        |FROM FifaContinentData
        |GROUP BY Nationality
        |ORDER BY TotalSalary DESC
        |LIMIT 3;
        |""".stripMargin

    }

    /**
     * Retrieve the club with the most valuable player.
     *
     * This query calculates the maximum value of players in each club and
     * returns the club with the highest maximum player value, representing
     * the most valuable player in that club.
     *
     * @return A DataFrame containing the club with the most valuable player and the
     * corresponding total value, ordered by the total value in descending order.
     */
      //TODO: FIX THIS
    val theMostValuablePlayerQuery =
      """
        |SELECT Club, MAX(Value) AS Total
        |FROM FifaContinentData
        |GROUP BY Club
        |ORDER BY Total DESC
        |LIMIT 1;
        |""".stripMargin

    /**
     * Retrieve the top five clubs based on total player salaries.
     *
     * This query calculates the total salary spent by each club on their players,
     * sums up the player values for each club, and returns the top five clubs
     * with the highest total player salaries.
     *
     * @return A DataFrame containing the top five clubs with the highest total player salaries,
     * along with their corresponding total salary, ordered by the total salary in descending order.
     */
    val topFiveSalariesClub =
      """
        |SELECT Club, SUM(Value) AS TotalSalary
        |FROM FifaContinentData
        |GROUP BY Club
        |ORDER BY TotalSalary DESC
        |LIMIT 5;
        |""".stripMargin

    /**
     * Retrieve the continent with the highest average FIFA Score for players.
     *
     * This query calculates the average FIFA Score for players from each continent,
     * groups the data by the continent while mapping 'SA' and 'NA' continents to 'America',
     * and returns the continent with the highest average FIFA Score.
     *
     * @return A DataFrame containing the continent with the highest average FIFA Score,
     * along with the corresponding average FIFA Score, ordered by the average FIFA Score in descending order.
     */
    val bestFifaContinentPlayerQuery =
      """
        |SELECT CASE
        |WHEN Continent IN ('SA', 'NA') THEN 'America'
        |ELSE Continent
        |END AS CombinedContinent,
        |AVG(`Fifa Score`) AS AverageFifaScore
        |FROM FifaContinentData
        |GROUP BY CASE
        |WHEN Continent IN ('SA', 'NA') THEN 'America'
        |ELSE Continent
        |END
        |ORDER BY AverageFifaScore DESC
        |LIMIT 1; """.stripMargin

    /**
     * Execute SQL queries to extract summarized insights from the FIFA player dataset.
     *
     * This section of code executes a series of SQL queries on the FIFA player dataset
     * to extract summarized insights. The queries cover different aspects, such as
     * calculating the top three countries achieving the highest income through their players,
     * identifying the most valuable club, finding the top five clubs with the highest salary spending,
     * and determining the continent with the best average FIFA Score for players.
     *
     * @return DataFrames containing the results of the executed SQL queries.
     */
    val aggregatedIncomeResult = spark.sql(topThreeCountriesQuery)
    val aggregatedValueResult = spark.sql(theMostValuablePlayerQuery)
    val aggregatedSalaryResult = spark.sql(topFiveSalariesClub)
    val bestFifaContinentResult = spark.sql(bestFifaContinentPlayerQuery)

    /**
     * Write Query Results to CSV Files
     *
     * This section of code takes the DataFrames containing the summarized insights extracted
     * from the FIFA player dataset and writes the results to separate CSV files. Each query's
     * output is saved to a distinct file location specified by the Constants object.
     *
     * @param aggregatedIncomeResult  DataFrame containing the top three countries achieving the highest income.
     * @param aggregatedValueResult   DataFrame containing the information about the most valuable club.
     * @param aggregatedSalaryResult  DataFrame containing the top five clubs with the highest salary spending.
     * @param bestFifaContinentResult DataFrame containing the continent with the best average FIFA Score for players.
     */
    writeResultToCSV(aggregatedIncomeResult, utils.Constants.query1_Output)
    writeResultToCSV(aggregatedValueResult, utils.Constants.query2_output)
    writeResultToCSV(aggregatedSalaryResult, utils.Constants.query3_Output)
    writeResultToCSV(bestFifaContinentResult, utils.Constants.query4_Output)

    /**
     * Remove DataFrame from Memory Cache
     *
     * This line of code is used to release the DataFrame "FifaWithContinentData" from memory cache.
     * The DataFrame was previously cached to improve performance during computations.
     * Unpersisting the DataFrame frees up memory resources.
     */
    FifaWithContinentData.unpersist()

    /**
    *Stop spark session
     */
    spark.stop()
  }
}
