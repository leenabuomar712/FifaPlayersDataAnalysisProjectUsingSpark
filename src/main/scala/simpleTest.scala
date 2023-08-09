package application
import java.io.{File, PrintWriter}

object simpleTest {
  def main(args: Array[String]): Unit = {
    val outputFilePath = "output.csv"

    // Sample data
    val data = List(
      ("John", 25, "New York"),
      ("Alice", 28, "Los Angeles"),
      ("Bob", 22, "Chicago")
    )

    // Write data to CSV file
    val pw = new PrintWriter(new File(outputFilePath))
    pw.write("Name,Age,City\n") // Write header

    data.foreach { case (name, age, city) =>
      pw.write(s"$name,$age,$city\n")
    }

    pw.close()

    println(s"Data has been written to $outputFilePath")
  }
}
