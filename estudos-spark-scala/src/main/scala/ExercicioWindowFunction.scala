
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ExercicioWindowFunction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ExercicioWindowFunction")
      .master("local[*]")
      .getOrCreate()

    val dfFuncionarios = spark.read
      .parquet("dados/entrada/funcionarios_10k.parquet")

    val dfDepartamentos = spark.read
      .parquet("dados/entrada/departamentos.parquet")

    val dfChefes = spark.read
      .parquet("dados/entrada/chefes.parquet")

    dfFuncionarios.show(20)

    val funcWindow = Window.partitionBy("id_departamento")

    val media_idade = dfFuncionarios.withColumn(
      "media_idade",
      avg(col("idade")).over(funcWindow)
    )
    media_idade.show(5)

    spark.stop()

  }
}