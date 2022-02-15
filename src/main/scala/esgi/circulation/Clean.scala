package esgi.circulation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Clean {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = SparkSession
        .builder()
        .appName("Simple Spark App")
        .master("local[*]")
        .getOrCreate()

    val inputFile = args(0)
    val outputFile = args(1)
    // TODO : lire son fichier d'input
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputFile)

    // TODO : ajouter 3 colonnes à votre dataframe pour l'année, le mois et le jour
    df
      .withColumn("date", date_format(col("date_debut"), "MM-dd-yyyy"))
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))

    // TODO : écrire le fichier en parquet et partitionné par année / mois / jour
    df.write
      .partitionBy("year","month","day")
      .parquet(outputFile)
  }
}