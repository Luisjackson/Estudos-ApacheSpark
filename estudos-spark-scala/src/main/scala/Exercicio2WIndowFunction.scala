//🏋️‍♂️ O Desafio: Identificando os Maiores Salários e a Discrepância
//A diretoria quer fazer uma auditoria na folha de pagamento.
// Eles precisam de um relatório dos funcionários, mas querem duas novas colunas para análise:
//
//ranking_salario: Um ranking que diga quem ganha mais dentro de cada departamento
// (o maior salário do departamento recebe o número 1, o segundo maior o número 2, e assim por diante).
//
//maior_salario_depto: Uma coluna que repita, em todas as linhas daquele departamento,
// qual é o maior salário daquele setor. (Com isso, eles conseguem subtrair o salário
// do funcionário do maior salário para ver a distância entre eles).
//
//🎯 Sua Missão no Código Scala:
//  Crie uma janela chamada janelaRanking que particione por id_departamento e ordene pelo salario de forma decrescente (.desc).
//
//Crie uma segunda janela chamada janelaMaior que apenas particione por id_departamento
// (sem ordenar, afinal, o maior salário do setor é o mesmo independente da ordem).
//Para o ranking, use a função row_number() combinada com a janelaRanking.
//  Para o maior salário, use a função max("salario") combinada com a janelaMaior.
//  Dê um .show(20) e analise o resultado!

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Exercicio2WIndowFunction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Exercicio2WindowFunction")
      .master("local[*]")
      .getOrCreate()

    val dfFuncionarios = spark.read.parquet("dados/entrada/funcionarios_10k.parquet")
    val dfDepartamentos = spark.read.parquet("dados/entrada/departamentos.parquet")
    val dfChefes = spark.read.parquet("dados/entrada/chefes.parquet")

    dfFuncionarios.show(5)

    val janela = Window
      .partitionBy("id_departamento")
      .orderBy(col("salario").desc)

    val funcionariosRanking = dfFuncionarios.withColumn(
      "ranking_salario",
      row_number().over(janela)
    )

    funcionariosRanking.show(5)

    val maiorSalario = funcionariosRanking.withColumn(
      "maior_salario_depto",
      max(col("salario")).over(janela)
    )

    maiorSalario.show(10)

    val diferencaSalarial = maiorSalario.withColumn(
      "diferença_salarial",
      round(col("maior_salario_depto") - col("salario"), 2)
    )

    //Caso eu precise fazer mudança em mais de 3 colunas
    // Vi que é uma boa prática utilizar Select, ao inves de with column.

  val relatorio = dfFuncionarios.select(
    col("id"),
    col("nome"),
    col("id_departamento"),
    col("salario"),
    col("idade"),

    row_number().over(janela).as("ranking_salario"),
    max(col("salario")).over(janela).as("maior_salario_depto"),
    round(col("maior_salario_depto") - col("salario"), 2).as("diferença_salarial")
  )

    println("Relatorio com WithColumn")
    diferencaSalarial.show(25)
    diferencaSalarial.explain()

    println("Relatorio com Select")
    relatorio.show(25)
    relatorio.explain()



  }
}