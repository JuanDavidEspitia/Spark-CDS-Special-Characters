package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object OperationsOnPairRDD
{
  def main(args: Array[String]): Unit =
  {
    /**
     * PairRDDFunctions
     *
     * Spark define la clase PairRDDFunctions con varias funciones para
     * trabajar con Pair RDD o RDD par clave-valor.
     * Los RDD de par son útiles cuando necesita aplicar transformaciones como partición hash,
     * operaciones de configuración, uniones, etc.
     *
     * */
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Trasnformation RDD")
      .getOrCreate()


    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China", "Colombia Peru Bolivia Brazil ")
    )
    val wordsRdd = rdd.flatMap(_.split(" "))
    val pairRDD = wordsRdd.map(f=>(f,1))
    pairRDD.foreach(println)

    /**
     * distinct – Retorna las llaves distintas.
     *
     * */
    println("********************************    DISTINCT   *********************************")
    pairRDD.distinct().foreach(println)

    /**
     * sortByKey – Transformation returns an RDD after sorting by key
     * */
    println("Sort by Key ==>")
    val sortRDD = pairRDD.sortByKey()
    sortRDD.foreach(println)

    /**
     * reduceByKey – Transformation returns an RDD after adding value for each key.
     * */
    println("Reduce by Key ==>")
    val wordCount = pairRDD.reduceByKey((a,b)=>a+b)
    wordCount.foreach(println)

    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by Key ==> wordcount")
    val wordCount2 = pairRDD.aggregateByKey(0)(param1,param2)
    wordCount2.foreach(println)

    /**
     * keys – Return RDD[K] with all keys in an dataset
     * */
    println("Keys ==>")
    wordCount2.keys.foreach(println)

    /**
     * values – return RDD[V] with all values in an dataset
     *
     * */
    println("Keys ==>")
    wordCount2.keys.foreach(println)

    /**
     * count – This is an action function and returns a count of a dataset
     * */
    println("Count :"+wordCount2.count())




  }

}
