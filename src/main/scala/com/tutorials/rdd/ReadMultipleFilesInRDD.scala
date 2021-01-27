package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReadMultipleFilesInRDD
{
  def main(args: Array[String]): Unit = {
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
     * Spark Leer todos los archivos de texto de un directorio en un solo RDD
     * */
    val rdd = spark.sparkContext.textFile("input/textfiles_rdd/*")
    rdd.foreach(f => {
      println(f)
    })

    /**
     * Podemos usar la funcion Collect y Foreach para imprimir el contenido del RDD
     * */
    println("Otra forma de imprimir el contenido del RDD")
    rdd.collect.foreach(f=>{
      println(f)
    })

    /**
     * El método wholeTextFiles (). devuelve un RDD [Tuple2].
     * donde el primer valor (_1) en una tupla es un nombre de archivo y
     * el segundo valor (_2) es el contenido del archivo.
     * */
    println("Imprimimos con el metodo wholeTextFiles (). Devuelve una tupla \n" +
      "NombreRuta_Archivo, Contenido_Archivo")
    val rddWhole = spark.sparkContext.wholeTextFiles("input/textfiles_rdd/*")
    println("                              NombreRuta_Archivo                                            |           Contenido_Archivo     ")
    rddWhole.foreach(f=>{
      println(f._1+" --> "+f._2)
    })

    /**
     * Leer multiples archivos de texto en un solo RDD
     * */

















  }

}
