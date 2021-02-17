package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SaltosDeLinea
{

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Trasnformation RDD")
      .getOrCreate()

    val path = "gs://gcp-st-cds-pre-landing-zone/directorio"

    val rdd = spark.sparkContext.textFile("input/hdfs_files/file_hdfs_table.txt")
    println(rdd.count())
    rdd.foreach(println)

    /**
     * Buscar un valor en especifico dentro de un RDD
     * Para este ejemplo buscamos el numero 4
     */
    val rddFind = rdd.zipWithIndex.filter(linea => linea._1.contains("æAlexandriaæ"))
    rddFind.foreach(println)

    /**
     * El ultimo valor que pone es el Index donde encuentra ese registro
     */
    val rdd2 = rdd.zipWithIndex.filter(linea => linea._1.contains("15721584"))
    rdd2.foreach(println)


    /**
     * Busca en el RDD saltos de linea y que Index/Linea estan
     */
    val rddSplit = rdd.map( line => (line.split("æ"))).zipWithIndex.filter(line => line._1(0) == "")
    println("---------    RDD Split   ------")
    rddSplit.foreach(println)



    /***
     * Metodo 2 para encontrar los saltos de linea
     */
    val path01 = "input/hdfs_files/file_hdfs_table.txt"
    val delimiter = "æ"
    val rddtmp01 = spark.sparkContext.textFile(path01)
    println("La cantidad de Registros que tiene el RDD es: " + rddtmp01.count())
    // Separamos cada columna por el delimitador
    val rddtxt01 = rddtmp01.map( f => {f.split(delimiter)})
    //rddtxt01.collect().foreach(println)
    /**
     * En una varibale guardamos la cantidad de saltos de linea que tiene el RDD
     */
    val rddCount = rddtxt01.filter( x => x(0) == "").count()
    println(rddCount)




















  }
}
