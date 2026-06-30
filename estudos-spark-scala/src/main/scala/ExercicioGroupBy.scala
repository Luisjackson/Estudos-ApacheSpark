import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExercicioGroupBy {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ExercicioGroupBy")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dados/entrada/funcionarios_10k.csv")


//
    val agrupadoPorDepartamento = df.groupBy("departamento").count()

    println("Quantidade de funcionario por Departamento")
    agrupadoPorDepartamento.show()

    val maiorSalarioPorDepto = df.groupBy("departamento").max("salario")
    maiorSalarioPorDepto.show()

    val relatorioCompleto = df.groupBy("departamento")
      .agg(
        count("id").as("total_funcionarios"),
        round(sum("salario"), 2).as("salario_total_setor"),
        avg("salario").as("media_salarial_setor"),
        max("salario").as("maior_salario")
      )

    relatorioCompleto.printSchema()

    relatorioCompleto.show()

//O Cenário: Auditoria da Folha de Pagamento para a Diretoria
//A diretoria da empresa quer entender como estão divididos os custos com salários,
    // mas eles não querem olhar a empresa inteira de uma vez. Eles estão focados apenas nas pessoas que têm até 40 anos (inclusive).
//
//Além disso, eles não querem ver dados de estagiários ou salários muito baixos, então você deve desconsiderar qualquer funcionário que ganhe menos de 3000.0.
//Sude sua Missão (O que o seu código deve fazer):
//Filtrar a base antes de agrupar, mantendo apenas os funcionários com idade menor ou igual a 40 AND com salario maior ou igual a 3000.0.
//Agrupar os dados por departamento.
//Gerar um relatório usando o .agg() contendo:
//A quantidade de funcionários daquele depto que entraram na regra (chame de "total_colaboradores").A soma total gasta com salários naquele depto (use a função sum("salario") e chame de "custo_total_salario").
//A idade do funcionário mais velho desse grupo em cada depto (use a função max("idade") e chame de "maior_idade_grupo").
//Mostrar o resultado na tela com o .show().

  val dfQuarentoes = df.filter(col("idade") <= 40)

    val salariosMedianos = dfQuarentoes.filter(col("salario") > 3000)

    val agrupamentoPorDepto = salariosMedianos.groupBy("departamento").agg(
      count("id").as("total_colaboradores"),
      sum("salario").as("custo_total"),
      max("idade").as("maior_idade_grupo")
    )

    agrupamentoPorDepto.show()


  }
}
