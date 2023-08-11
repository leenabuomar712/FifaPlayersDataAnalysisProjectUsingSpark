FIFA Players Analysis
This is a Spark application that demonstrates data preprocessing and analysis using the Apache Spark framework. The application processes FIFA player data and performs various analytical tasks on the data.

Getting Started
To run the application, follow these steps:

1. Clone the repository to your local machine:
git clone https://github.com/your-username/SparkExample.git

2. Make sure you have Apache Spark installed on your machine.

3. Update the paths to your dataset files in the code:
    - playerData: Update the path to the FIFA player dataset CSV file.
    - countriesData: Update the path to the countries' continent dataset CSV file.
    - newDatasetPath: Update the path to the updated salary dataset CSV file.
    - s3OutputPath: specify an S3 output path. don't forget to create a free tier AWS account
    - updatedSalaryOutputPath: Specify the directory where the processed data will be saved.

4. Open a terminal and navigate to the project directory.

5. Compile and run the application using the following command:
spark-submit --class application.SparkExample --master local path/to/SparkExample.jar

6. The application will preprocess the dataset, perform various analytical tasks, and save the results in the specified output directory.


Preprocessing and Analysis
The application performs the following preprocessing steps on the dataset:

- Removes euro signs from value columns.
- Converts 'M' to 1000000 and 'K' to 1000 in value and salary columns.
- Removes special characters from value and salary columns.
- Converts name, nationality, and club columns to lowercase.
- Removes spaces from various columns.

The application then performs the following analytical tasks:

- Aggregates and ranks countries by player income.
- Determines the most valuable club based on player value.
- Ranks clubs by total salary spending.
- Identifies the continent with the highest average FIFA score.

Dependencies
- Apache Spark 