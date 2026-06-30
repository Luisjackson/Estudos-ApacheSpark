import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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


//    val tamanhosDfs = Map(
//      "Funcionario" -> s"${dfFuncionarios.count()}",
//      "Chefe" -> s"${dfChefes.count()}",
//      "Departamento" -> s"${dfDepartamentos.count()}"
//    )
//
//    dfFuncionarios.show(5)
//
//    dfChefes.show(5)
//
//    dfDepartamentos.show(5)

    val funcComDepartamento = dfFuncionarios.join(
      dfDepartamentos,
      dfFuncionarios("id_departamento") === dfDepartamentos("id_depto"),
      "left"
    )

    funcComDepartamento.drop("id_depto").show(5)

    val nomeDepto = funcComDepartamento.filter((col("id_departamento") === 4) && col("salario") < 10000)

    nomeDepto.select(col("id"), col("nome"), col("salario"), col("nome_depto")).show(5)


  }
}