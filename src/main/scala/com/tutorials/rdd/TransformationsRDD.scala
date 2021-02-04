package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TransformationsRDD
{
  def main(args: Array[String]): Unit = {
    /**
     * Inicilizamos los Logger
     * */
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)

    /**
     * Por definicion los RDD son Inmutables, cada transformacion genera un nuevo RDD
     * Tipos de Transformaciones
     * 1. Transformación estrecha ->  map(), mapPartition(), flatMap(), filter(), union()
     * 2. Transformación más amplia -> groupByKey(), aggregateByKey(), aggregate(), join(), repartition()
     *
     * */
    /**
     * Iniciamos la Session de Saprk
     * */
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Trasnformation RDD")
      .getOrCreate()

    // Declaramos el SparkContext
    val sc = spark.sparkContext

    /**
     * Creamos un RDD de Strings y leemos el archivo plano con el texto
     *
     * */
    val rdd:RDD[String] = sc.textFile("input/word_count/word_count.txt")

    /**
     * TYrasnformacion flatMap()
     * En el siguiente ejemplo, primero divide cada registro por espacio en un RDD
     * y finalmente lo aplana. El RDD resultante consta de una sola palabra en cada registro
     *
     * */
    val rdd2 = rdd.flatMap(f=>f.split(" "))
    println("flatMap() convierte por medio del delimitador en un Registro \n")
    rdd2.foreach(println)

    /**
     * Trasnformacion map()
     *
     * La transformación map () se utiliza para aplicar cualquier operación compleja
     * como agregar una columna, actualizar una columna e.t.c, la salida de las transformaciones
     * del mapa siempre tendría el mismo número de registros que la entrada.
     *
     * */

    /**
     * En nuestro ejemplo de recuento de palabras, estamos agregando una nueva columna
     * con valor 1 para cada palabra, el resultado del RDD es PairRDDFunctions
     * que contiene pares clave-valor, palabra de tipo String como clave y 1 de tipo Int como valor.
     * Para su comprensión, he definido la variable rdd3 con type.
     *
     * */
    println("##########################################################################################")
    println("A cada palabra le asigna un 1")
    val rdd3:RDD[(String,Int)]= rdd2.map(m=>(m,1))
    rdd3.foreach(println)

    /**
     * Trasnformacion reduceByKey()
     *
     * reduceByKey () fusiona los valores de cada clave con la función especificada.
     * En nuestro ejemplo, reduce la cadena de palabras aplicando la función de SUMA en valor.
     * El resultado de nuestro RDD contiene palabras únicas y su recuento.
     *
     * */
    println("*************************************************************************************")
    println("reduce la cadena de palabras aplicando la función de SUMA en valor. por la clave")
    val rddReduceByKey = rdd3.reduceByKey(_ + _)
    rddReduceByKey.foreach(println)

    /**
     * Trasnformacion sortByKey()
     * se usa para ordenar los elementos RDD en clave. En nuestro ejemplo,
     * primero, convertimos RDD [(String, Int]) a RDD [(Int, String]) usando la transformación del mapa
     * y aplicamos sortByKey que idealmente ordena en un valor entero.
     * Y finalmente, foreach con la declaración println imprime todas las palabras en RDD
     * y su recuento como par clave-valor en la consola.
     *
     * */
    println("********************************   sortByKey()   ***************************************")
    println("Trasnformacion sortByKey()")
    println("Cambiamos el orden de las columnas")
    val rdd6 = rddReduceByKey.map(a=>(a._2,a._1)).sortByKey()
    //Print rdd6 result to console
    rdd6.foreach(println)




  }





}
