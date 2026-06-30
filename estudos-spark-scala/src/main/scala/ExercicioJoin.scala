import org.apache.spark.sql.SparkSession

object ExercicioJoin {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ExercicioJoin")
      .master("local[*]")
      .getOrCreate()


    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dados/entrada/chefes.csv")

    df.write.mode("overwrite").parquet("dados/entrada/chefes.parquet")

    val tamanho = df.count()

    println(tamanho)


    spark.stop()

  }
}