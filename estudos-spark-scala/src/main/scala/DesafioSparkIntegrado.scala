import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * 🏆 DESAFIO INTEGRADO APACHE SPARK (SCALA)
  *
  * Este desafio foi projetado para integrar e consolidar os conceitos aprendidos:
  * - Leitura de Parquet
  * - select e filter
  * - withColumn (com condicionais when/otherwise)
  * - join (multi-tabelas e tipos de join)
  * - Window Functions (ranking e agregações analíticas)
  * - groupBy e agg (métricas consolidadas por departamento)
  *
  * Cenário:
  * A diretoria quer realizar um estudo aprofundado sobre a folha salarial dos funcionários experientes.
  * Você precisa gerar um relatório final cruzando funcionários, departamentos e chefes, rankeando os salários
  * e calculando a diferença salarial em relação à média do departamento.
  */
object DesafioSparkIntegrado {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DesafioSparkIntegrado")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val dfFuncionarios = spark.read.parquet("dados/entrada/funcionarios_10k.parquet")
    val dfDepartamentos = spark.read.parquet("dados/entrada/departamentos.parquet")
    val dfChefes = spark.read.parquet("dados/entrada/chefes.parquet")

//    dfFuncionarios.show(5)

    /*
     * ✍️ INSTRUÇÕES DE EXECUÇÃO:
     * Complete os passos abaixo substituindo os placeholders `???` ou escrevendo sua lógica.
     */

    // =========================================================================
    // PASSO 1: Seleção e Filtragem
    // - Filtre os funcionários com idade > 25 e salário > 3500.00.
    // - Selecione apenas: id, nome, id_departamento, salario, idade.
    // =========================================================================
    val dfFiltrado = dfFuncionarios
      .filter(col("idade") > 25 && col("salario") > 3500)

//    dfFiltrado.show(5)
    // =========================================================================
    // PASSO 2: Criação de Colunas com withColumn (Classificação Etária)
    // - Adicione a coluna "faixa_etaria" usando a seguinte regra:
    //   - idade < 35: "Jovem Adulto"
    //   - idade >= 35 e idade <= 50: "Adulto"
    //   - idade > 50: "Sênior"
    // =========================================================================
    val dfComFaixaEtaria = dfFiltrado.withColumn(
      "faixa_etaria",
      when(col("idade") < 35, "Jovem Adulto")
        .when(col("idade") >= 35 && col("idade") <= 50, "Adulto")
        .otherwise("Senior")
      )

//    dfComFaixaEtaria.show(5)


    // =========================================================================
    // PASSO 3: Junção de Dados (Joins)
    // - Junte o DataFrame com `dfDepartamentos` (id_departamento com id_depto).
    // - Junte o resultado com `dfChefes` (id_depto com id_depto).
    // - ATENÇÃO: Escolha com cuidado se deve usar Inner Join ou Left Join!
    //   Alguns departamentos podem não ter chefe. Queremos mantê-los no relatório?
    // =========================================================================
    val dfCruzado = dfComFaixaEtaria
      .join(dfDepartamentos, dfComFaixaEtaria("id_departamento") === dfDepartamentos("id_depto"), "inner")
      .join(dfChefes, "id_depto", "left")

    dfCruzado.show(5)


    // =========================================================================
    // PASSO 4: Window Functions (Análise e Ranking)
    // - Crie uma Window (janelaDepto) particionada por departamento ("nome_depto").
    // - Crie outra Window (janelaOrdenadaDepto) particionada por departamento e ordenada pelo salário desc.
    // - Usando as janelas, calcule:
    //   1. "ranking_salario_depto": o ranking de salário dentro do departamento (use dense_rank ou row_number).
    //   2. "media_salario_depto": a média salarial do respectivo departamento.
    //   3. "diferenca_media_depto": a diferença entre o salário do funcionário e a média do departamento dele (salario - media_salario_depto).
    // =========================================================================
    val janelaDepto = Window.partitionBy("id_departamento")
    val janelaOrdenadaDepto = Window.partitionBy("id_departamento").orderBy(col("salario").desc)

    val dfAnalítico = dfCruzado.withColumn(
      "ranking_salario_depto",
      dense_rank().over(janelaOrdenadaDepto),
    )

    val mediaSalarialDepto = dfAnalítico.withColumn(
      "media_salario_depto",
      avg(col("salario")).over(janelaDepto)
    )

    val diferencaSalarial = mediaSalarialDepto.withColumn(
      "diferenca_media_depto",
      round(col("salario") - avg(col("salario")).over(janelaDepto), 2)
    )


    diferencaSalarial.show(30, truncate = false)

    dfAnalítico.show(5)




    spark.stop()
  }
}
