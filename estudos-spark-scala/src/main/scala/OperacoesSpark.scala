import org.apache.spark.sql._

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
      .csv("dados/entrada/funcionarios_10k.csv")

    df.printSchema()
    
    spark.stop()
  }

}