package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountExample
{
  def main(args:Array[String]): Unit = {
    /**
     * Inicilizamos los Logger
     * */
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)

    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count don Quijote")
      .getOrCreate()

    val sc = spark.sparkContext

    println("Leemos el archivo plano con el texto de Libro Don Quijote de la Mancha")
    val rdd:RDD[String] = sc.textFile("input/word_count/quijote.txt")
    println("initial partition count:"+rdd.getNumPartitions)

    println("Reparticionamos el RDD del Quijote en 4 partes")
    val reparRdd = rdd.repartition(4)
    println("re-partition count:"+reparRdd.getNumPartitions)

    //rdd.coalesce(3)
    println("*****************************************   INICIO   *******************************************")
    println("Imprimimos el RDD")
    rdd.collect().foreach(println)
    println("***********************************************   FIN   *************************************")

    // rdd flatMap transformation
    println("Separamos cada palabra delimitada por espacio, y lo ponemos como registro")
    val rdd2 = rdd.flatMap(f=>f.split(" "))
    rdd2.foreach(f=>println(f))

    //Create a Tuple by adding 1 to each word
    println("*************************************************************************************")
    println("Creamos una tupla de cada palabra y le agragamos un 1")
    val rdd3:RDD[(String,Int)]= rdd2.map(m=>(m,1))
    rdd3.foreach(println)

    //Filter transformation
    println("***************************************  INICIO FILTER  **********************************************")
    println("Filtramos los valores de la primera columna que inicien con 'a'")
    val rdd4 = rdd3.filter(a=> a._1.startsWith("a"))
    rdd4.foreach(println)
    println("***************************************  FIN FILTER  **********************************************")

    //ReduceBy transformation
    val rdd5 = rdd3.reduceByKey(_ + _)
    rdd5.foreach(println)

    //Swap word,count and sortByKey transformation
    val rdd6 = rdd5.map(a=>(a._2,a._1)).sortByKey()
    println("Final Result")

    //Action - foreach
    rdd6.foreach(println)

    //Action - count
    println("Count : "+rdd6.count())

    //Action - first
    val firstRec = rdd6.first()
    println("First Record : "+firstRec._1 + ","+ firstRec._2)

    //Action - max
    val datMax = rdd6.max()
    println("Max Record : "+datMax._1 + ","+ datMax._2)

    //Action - reduce
    val totalWordCount = rdd6.reduce((a,b) => (a._1+b._1,a._2))
    println("dataReduce Record : "+totalWordCount._1)
    //Action - take
    val data3 = rdd6.take(3)
    data3.foreach(f=>{
      println("data3 Key:"+ f._1 +", Value:"+f._2)
    })

    //Action - collect
    val data = rdd6.collect()
    data.foreach(f=>{
      println("Key:"+ f._1 +", Value:"+f._2)
    })

    //Action - saveAsTextFile
    println("Guardamos el resultado en un archivo .out")
   rdd5.saveAsTextFile("output/salidaWordCount")
    println("El resultado se escribio Satisfactoriamente   -->> OK")

  }



}
