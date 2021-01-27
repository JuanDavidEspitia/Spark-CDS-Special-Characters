package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._

object CaracteresEspeciales
{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)
    // Creacion de la variablee para punto de partida de la aplicacion
    val spark = SparkSession.builder().appName("DataIngest-CDS").master("local[*]").getOrCreate()
    // Declaramos la variable de tiempo para calcular cuando se demora la ejecucion del artefacto
    val startTimeMillis = System.currentTimeMillis()
    println(startTimeMillis) // Imprimimos el tiempo de inicio en segundos



  }

}
