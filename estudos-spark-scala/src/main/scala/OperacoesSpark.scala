import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object OperacoesSpark {

  case class Funcionaro(id: Long, nome: String, departamento: String, salario: Double, idade: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("OperacoesSpark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dados/entrada/funcionarios_10k.csv")

    val colunaID = df.select(col("id"), col("nome"), col("departamento"))

    val colunaIdPares = df.filter(col("departamento") === "TI" && col("salario") > 10000 && col("id") % 2 === 0)

    colunaIdPares.select(col("id"), col("nome"), col("salario")).show()

      df.printSchema()
    spark.stop()
  }

}