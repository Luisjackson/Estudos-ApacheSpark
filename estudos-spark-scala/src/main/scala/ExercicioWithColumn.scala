
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExercicioWithColumnn {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ExercicioWithColumn")
      .master("local[*]")
      .getOrCreate()

    val dfFuncionarios = spark.read
      .parquet("dados/entrada/funcionarios_10k.parquet")

    val dfDepartamentos = spark.read
      .parquet("dados/entrada/departamentos.parquet")

    val dfChefes = spark.read
      .parquet("dados/entrada/chefes.parquet")

    dfFuncionarios.show(5)
    val funcionarioSalarioAnual = dfFuncionarios.withColumn("salario_anual", col("salario") * 12)

    val funcMaiusculo = dfFuncionarios.withColumn("nome", upper(col("nome")))

    val nivel = dfFuncionarios.withColumn(
      "Nivel_cargo",
      when(col("salario") < 5000, "Junior")
        .when(col("salario") >=5000, "Pleno")
        .otherwise("Senior")
    )

    val salarioAnual = funcMaiusculo.withColumn(
      "salario",
      col("salario") * 12
    )


  val salarioAnualModificado = funcMaiusculo.select(
    col("id"),
    upper(col("nome")),
    col("salario") * 12
  )

    salarioAnualModificado.show(5)
    nivel.show(5)

    funcMaiusculo.show(5)

    salarioAnual.show(5)

    spark.stop()

  }
}