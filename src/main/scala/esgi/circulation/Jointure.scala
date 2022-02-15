package esgi.circulation

import org.apache.spark.sql.SparkSession


object Jointure {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = SparkSession
      .builder()
      .appName("Simple Spark App")
      .master("local[*]")
      .getOrCreate()

    val inputFile = args(0)
    val joinFile = args(1)
    val outputFile = args(2)
    // TODO : lire son fichier d'input et son fichier de jointure
    val df = spark.read.parquet(inputFile)
    val joinDf = spark.read.csv(inputFile)
    // TODO : ajouter ses transformations Spark avec au minimum une jointure et une agrégation
    val outDf = ???
    // TODO : écrire le résultat dans un format pratique pour la dataviz
    outDf.write.parquet(outputFile)
  }
}