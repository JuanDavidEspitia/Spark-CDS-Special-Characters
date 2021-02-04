package com.tutorials.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD
{

  def main(args: Array[String]): Unit =
  {
    /**
     * Iniciamos la Session de Saprk
     * */
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark CreateEmptyRDD")
      .getOrCreate()

    /**
     * Creamos un RDD vacio
     * */
    val rdd = spark.sparkContext.emptyRDD // creates EmptyRDD[0]
    /**
     * Creamos un RDD vacio de Strings
     * */
    val rddString = spark.sparkContext.emptyRDD[String] // creates EmptyRDD[1]

    println(rdd)
    println(rddString)
    println("Num of Partitions: "+rdd.getNumPartitions) // returns o partition

    /**
     * Funcion saveAsTextFile() guarda como archivo de Texto el RDD
     * */
    //rddString.saveAsTextFile("test.txt")

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Numero de Particiones: "+rdd2.getNumPartitions)

    //rdd2.saveAsTextFile("test2.txt")

    // Pair RDD

    type dataType = (String,Int)
    var pairRDD = spark.sparkContext.emptyRDD[dataType]
    println(pairRDD)



  }
}
