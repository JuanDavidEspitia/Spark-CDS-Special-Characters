package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._

object CaracteresEspeciales
{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)
    // Creacion de la variablee para punto de partida de la aplicacion
    //val spark = SparkSession.builder().appName("DataIngest-CDS").master("local[*]").getOrCreate()
    val conf: SparkConf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("LimipezaDeCaracteres")
        .set("spark.driver.host", "localhost")
    val sc: SparkContext = new SparkContext(conf)
    // Declaramos la variable de tiempo para calcular cuando se demora la ejecucion del artefacto
    val startTimeMillis = System.currentTimeMillis()
    println(startTimeMillis) // Imprimimos el tiempo de inicio en segundos

    /*
    * Creamos una variable donde cargamos el archivo que contiene el salto de linea
    * */
    val texpath = "/home/adseglocdom/Documentos/test_saltos/ODS_FINMES_ATAHORROS.txt"
    //val rdd = sc.sparkContext.textFile(texpath)
    //val rddsplit = rdd.map(linea => (linea.split("|")))

    // Filtramos las lineas vacias
    //val rddLineasVacias = rddsplit.filter(linea => linea(0) =="")

    /*
    * Con esta linea lo spliteamos y lo filtramos por los vacios
    * */
    //val rddfilter = rdd.map(linea => (linea.split("|"))).filter(linea => linea(0) == "")

    /*
    * Convertimos a Datafrma el RDD
    * */
   // val dfRdd = rddfilter.toDF()


  }

}
