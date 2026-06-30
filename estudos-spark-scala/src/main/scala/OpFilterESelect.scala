//O Cenário: O time de RH precisa de uma lista rápida para enviar os parabéns para a galera mais experiente da empresa.
//Sua Missão: Crie um novo DataFrame que contenha apenas os funcionários com mais de 50 anos. No resultado final, exiba apenas as colunas nome e idade.

//Exercício 2: O "Corte de Gastos"
//O Cenário: A diretoria está avaliando a folha de pagamento e quer mapear quem são os funcionários que não pertencem ao departamento de TI,
//mas que têm salários de nível sênior.
//Sua Missão: Filtre a base para encontrar todos os funcionários cujo departamento seja diferente de "TI"
//E que ganhem um salario maior ou igual a 8000.0. Mostre na tela as colunas nome, departamento e salario.
//
//
//Exercício 3:
//O Cenário: Imagine que a empresa está fazendo uma análise de fraude ou auditoria interna.
//Eles precisam cruzar dados de uma lista específica de departamentos suspeitos ou prioritários, focando em um público jovem.
//Sua Missão: Filtre a tabela para trazer apenas os funcionários que trabalham no departamento de "Vendas" OR "RH"
//(pode usar o operador ||), e que tenham menos de 30 anos. Para o relatório final ficar limpo, exiba todas as colunas menos a coluna id.


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OpFilterESelect {

  case class Funcionario(id: Long, nome: String, departamento: String, salario: Double, idade: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("OpFilterESelect")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dados/entrada/funcionarios_10k.csv")
      .as[Funcionario]

    val dfCinquentao = df.filter(col("idade") > 50)
    dfCinquentao.select(col("nome"), col("idade")).show(5)

    val funcionariosForaDoTI = df.filter(col("departamento") =!= "TI" && col("salario") >= 8000)
    funcionariosForaDoTI.select(col("nome"), col("departamento"), col("salario")).show(5)

    val funcVendasERh = df.filter((col("departamento") === "Vendas" || col("departamento") === "RH") && col("idade") < 30)

    funcVendasERh.drop("id").show(5)


    spark.stop()

  }
}