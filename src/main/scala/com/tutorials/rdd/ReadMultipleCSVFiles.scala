package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReadMultipleCSVFiles
{
  def main(args: Array[String]): Unit =
  {
    /**
     * Inicilizamos los Logger que solo muestre por consola ERROR y WARM
     * */
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)

    /**
     * Iniciamos la Session de Saprk
     * */
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    /**
     * Cargamos un archivo CSV en un RDD
     * */
    val rddFromFile = spark.sparkContext.textFile("input/csv_files_rdd/text01.csv")
    /**
     * deberíamos usar la transformación map () en RDD
     * donde convertiremos RDD [String] a RDD [ Array [String]
     * dividiendo cada registro por delimitador de coma.
     * El método map () devuelve un nuevo RDD en lugar de actualizar el existente
     * */
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })
    /**
     * Ahora, lea los datos de rdd usando foreach, dado que los elementos en RDD
     * son una matriz, necesitamos usar el índice para recuperar cada elemento de una matriz.
     * */
    rdd.foreach(f=>{
      println("Col1: "+f(0)+",Col2: "+f(1))
    })

    /**
     * En este caso, el método collect () devuelve el tipo Array [Array [String]]
     * donde el primer Array representa los datos RDD y el arreglo interno es un registro
     * */
    println("Hacemos uso del metodo Collect() que devuelve un Array[Array[String]]")
    rdd.collect().foreach(f=>{
      println("Col1: "+f(0)+",Col2: "+f(1))
    })
    /**
     * Omitir encabezado del archivo CSV
     * */
    println("")
    println("Omitimos el encabezado de los archivos CSV")
    val rddSkipHeader = rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    rddSkipHeader.collect().foreach(f=>{
      println("Col1: "+f(0)+",Col2: "+f(1))
    })

    /**
     * Leer multiples archivos CSV, de diferentes rutas en un solo RDD
     * */
    val rdd4 = spark.sparkContext.textFile("input/csv_files_rdd/text01.csv,input/csv_files_rdd/text02.csv")
    println("Leer multiples archivos CSV, de diferentes rutas en un solo RDD")
    rdd4.foreach(f=>{
      println(f)
    })

    /**
     * Leer todos los archivos CSV de un directorio en un solo CSV
     * */
    val rdd3 = spark.sparkContext.textFile("input/csv_files_rdd/*")
    println("Leer todos los archivos CSV de un directorio en un solo CSV")
    rdd3.foreach(f=>{
      println(f)
    })








  }

}
