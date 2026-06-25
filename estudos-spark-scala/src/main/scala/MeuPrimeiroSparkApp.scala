import org.apache.spark.sql._
import org.apache.log4j._

object MeuPrimeiroSparkApp {

  case class Funcionario(id: Long, nome: String, departamento: String, salario: Double, idade: Int)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("MeuPrimeiroSparkApp")
      .master("local[*]")
      .getOrCreate()

    val dadosRaw = spark.read
      .option("header", "true" ) // Existe um cabeçalho
      .option("inferSchema", "true") // Inferir o tipo das colunas
      .csv("dados/entrada/funcionarios_10k.csv")

    val tamanhoDadosRaw = dadosRaw.count()

    println("O arquivo tem : " + tamanhoDadosRaw + " linhas")
    spark.stop()


  }

}