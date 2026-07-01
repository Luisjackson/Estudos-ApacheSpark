//🏋️‍♂️ O Desafio: O Relatório da Linha de Frente (Nível Pleno)
//A diretoria quer um relatório unificado para auditar quem comanda cada equipe.
// Eles querem ver na tela uma tabela com as seguintes colunas:
//
// Nome do Funcionário (nome)
//Salário do Funcionário (salario)
//Nome do Departamento por extenso (nome_depto)
//Nome do Chefe daquele departamento (nome_chefe)
//
// Sua Missão no Código:
//  Faça o primeiro Join: Cruze a tabela de dfFuncionarios com a tabela de dfDepartamentos usando as chaves de departamento
//  Faça o segundo Join (Encadeado): No resultado do primeiro cruzamento, faça um novo Join com a tabela de dfChefes.
//  Escolha os tipos de Join certos: * Use o tipo de join que garanta que nenhum funcionário suma do relatório,
//  mesmo se o departamento dele for aquele "ID 99" inválido (que o Python gerou de propósito para testar seu código)
//  ou se o departamento dele não tiver chefe cadastrado (como o setor de Engenharia de Dados, que deixamos sem chefe).
//  Limpeza Final: Use o .select() para mostrar apenas as 4 colunas pedidas pela diretoria e dê um .show(20).


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Exercicio2Join {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Exercicio2Join")
      .master("local[*]")
      .getOrCreate()

    val dfFuncionarios = spark.read
      .parquet("dados/entrada/funcionarios_10k.parquet")

    val dfDepartamentos = spark.read
      .parquet("dados/entrada/departamentos.parquet")

    val dfChefes = spark.read
      .parquet("dados/entrada/chefes.parquet")

    val dfFinal = dfFuncionarios
      .join(broadcast(dfDepartamentos), dfFuncionarios("id_departamento") === dfDepartamentos("id_depto"), "left")
      .join(broadcast(dfChefes), dfFuncionarios("id_departamento") === dfChefes("id_depto"), "left")


//    println("Departamento")
//    dfDepartamentos.printSchema()
//
//    println("Funcionario")
//    dfFuncionarios.printSchema()
//
//    println("Chefe")
//    dfChefes.printSchema()
//
//    dfFinal.select(col("nome"), col("salario"), col("nome_depto"), col("nome_chefe")).show(5)

    dfFinal.filter(col("nome_chefe") =!= "NULL")
      .select(col("nome"), col("salario"), col("nome_depto"), col("nome_chefe")).show(5)

    dfFinal.explain()

    spark.stop()

  }
}