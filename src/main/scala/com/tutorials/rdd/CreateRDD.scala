package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreateRDD
{
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
      .appName("SparkCreateRDD")
      .getOrCreate()

    /**
     * Creamos un RDD spark desde una Seq o Lista, usando Parallelize
     * */
    val rdd=spark.sparkContext.parallelize(Seq(("Java", 20000),
      ("Python", 100000), ("Scala", 3000)))
    rdd.foreach(println)

    /**
     * Creamos un RDD desde un archivo de Texto
     * */
    val rdd2 = spark.sparkContext.textFile("input/textfiles_rdd/text01.txt")
    /**
     * Creamos un RDD desde un archivo de Texto con la funcion WholeTextFiles()
     * */
    val rdd3 = spark.sparkContext.wholeTextFiles("input/textfiles_rdd/text*.txt")
    rdd3.foreach(record=>println("Ruta/Nombre_Archivo : "+record._1+", Contenido_Archivo :"+record._2))

    /**
     * Creamos un RDD desde otro RDD
     * Para este caso le sumamos 1200 a cada valor de la segunda columna
     * */
    val rdd4 = rdd.map(row=>{(row._1,row._2+1200)})
    rdd4.foreach(println)

    /**
     * Desde DataFrames y DataSet existentes
     * Para convertir DataSet o DataFrame a RDD, simplemente use el método rdd ()
     * en cualquiera de estos tipos de datos.
     * */
    // Metodo 1 --> SparkSession.createDataFrame(RDD obj).
    val rddtodf = spark.createDataFrame(rdd)
    rddtodf.show(5)

    // Metodo 2
    //Using SparkSession.createDataFrame(RDD obj) and specifying column names.
    val rddtodfColumns = spark.createDataFrame(rdd).toDF("Lenguaje", "Valor")
    rddtodfColumns.show(5)

    // Metodo 3
    /* val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF()
     */












  }

}
