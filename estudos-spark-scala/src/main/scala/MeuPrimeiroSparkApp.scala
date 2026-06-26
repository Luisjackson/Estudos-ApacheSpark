import org.apache.spark.sql._
import org.apache.log4j._
import java.lang.Thread

object MeuPrimeiroSparkApp {

  case class Funcionario(id: Long, nome: String, departamento: String, salario: Double, idade: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MeuPrimeiroSparkApp")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dados/entrada/funcionarios_10k.csv")

    df.write.mode("overwrite").parquet("dados/saida")

    println("=== Acesse http://localhost:4040 no seu navegador ===") // Acessar UI do Spark no navegador
    Thread.sleep(600000)

    spark.stop()

  }


}