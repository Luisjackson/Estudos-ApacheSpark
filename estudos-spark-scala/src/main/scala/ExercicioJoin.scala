import org.apache.spark.sql.SparkSession

object ExercicioJoin {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ExercicioJoin")
      .master("local[*]")
      .getOrCreate()


    val dfFuncionarios = spark.read
      .parquet("dados/entrada/funcionarios_10k.parquet")

    val dfDepartamentos = spark.read
      .parquet("dados/entrada/departamentos.parquet")

    val dfChefes = spark.read
      .parquet("dados/entrada/chefes.parquet")


    val tamanhosDfs = Map(
      "Funcionario" -> s"${dfFuncionarios.count()}",
      "Chefe" -> s"${dfChefes.count()}",
      "Departamento" -> s"${dfDepartamentos.count()}"
    )

    println(tamanhosDfs)


    spark.stop()

  }
}