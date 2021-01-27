package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.test.TestHive.sparkContext

object RDDParallelize {
  def main(args: Array[String]): Unit = {

    /**
     * Inicilizamos los Logger
     * */
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)

    /**
     * Iniciamos la Session de Saprk
     * */
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    /**
     * Creamos un RDD con sparkContext y lo paralelizamos agregandole una lista de numeros
     * */
    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9))

    println("Number of Partitions: "+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first()) // Obtenemos el primer elemento del RDD
    println("Action: RDD converted to Array[Int] : ")
    /**
     * Convertimos el RDD en Array e inpirmimos sus elementos
     * */
    val rddCollect:Array[Int] = rdd.collect()
    rddCollect.foreach(println)

    // -------------------------------------------------------------

    /**
     * Declaramos el contexto de Spark
     * */
    val sc = spark.sparkContext

    /**
     * Creamos un RDD Vacio
     * */
    var rdd2 = sc.parallelize(Seq.empty[String])

    /**
     * Creamos un RDD paralelizable con una lista que tiene datos en un Rango de 1 a 1000
     * */
    val rdd4:RDD[Range] = sc.parallelize(List(1 to 1000))
    println("Number of Partitions of RDD4: "+rdd4.getNumPartitions)

    /**
     * Creamos el RDD5 apartir del RDD4 con el rango de numeros y lo
     * Particionamos en 5
     * */
    println(("Create to RDD5 with numPartition = 5"))
    val rdd5 = rdd4.repartition(5)
    println("Number of Partitions of RDD5 : "+rdd5.getNumPartitions)

    /**
     * Creamos el RDD6 apartir del RDD5
     * y lo convertimos en un array
     * */
    println("RDD6 convert to array, delimited by coma")
    val rdd6:Array[Range] = rdd5.collect()
    println(rdd6.mkString(","))

    val rdd7:Array[Array[Range]] = rdd5.glom().collect()
    println("After glom");
    rdd7.foreach(f=>{
      println("For each partition")
      f.foreach(f1=>println(f1))
    })

  }

}
