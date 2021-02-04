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
      .appName("SparkByExamples.com")
      .getOrCreate()



    



  }

}
