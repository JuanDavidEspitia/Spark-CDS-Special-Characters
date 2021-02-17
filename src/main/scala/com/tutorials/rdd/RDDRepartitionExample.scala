package com.tutorials.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object RDDRepartitionExample
{
  def main(args: Array[String]): Unit = {

    /**
     * SparkSession introducida en la versión Spark 2.0,
     * es un punto de entrada a la funcionalidad subyacente de Spark
     * para crear mediante programación Spark RDD, DataFrame y DataSet.
     * */
    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("RDDRepartitionExample")
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.WARN)


    /***
     * Una de las principales ventajas de Spark es que divide los datos en varias particiones
     * y ejecuta operaciones en todas las particiones de datos en paralelo, lo que nos permite
     * completar el trabajo más rápido. Mientras trabajamos con datos de partición, a menudo
     * necesitamos aumentar o disminuir las particiones según la distribución de datos.
     * Los métodos de repartición y fusión nos ayudan a repartir.
     *
     */
    val rdd = spark.sparkContext.parallelize(Range(0,20))
    println("From local[5] "+rdd.partitions.size)

    /**
     * spark.sparkContext.parallelize(Range(0,20),6) distribuye RDD en 6 particiones
     * y los datos se distribuyen como se muestra a continuación
     * */
    val rdd1 = spark.sparkContext.parallelize(Range(0,20), 6)
    println("parallelize : "+rdd1.partitions.size)

    rdd1.partitions.foreach(f=> f.toString)
    val rddFromFile = spark.sparkContext.textFile("input/textfiles_rdd/text*.txt",9)

    println("TextFile : "+rddFromFile.partitions.size)

    /**
     * El método Spark RDD repartition () se utiliza para aumentar o disminuir las particiones. El siguiente ejemplo reduce las particiones de 10 a 4 moviendo datos de todas las particiones.
     */
    rdd1.saveAsTextFile("output/tmp/partition")
    val rdd2 = rdd1.repartition(4)
    println("Repartition size : "+rdd2.partitions.size)

    rdd2.saveAsTextFile("output/tmp/re-partition")

    /**
     * Spark RDD  coalesce() se usa solo para reducir el número de particiones.
     * Esta es una versión optimizada o mejorada de repartition()
     * donde el movimiento de los datos a través de las particiones es menor usando coalesce.
     */
    val rdd3 = rdd1.coalesce(4)
    println("Repartition size : "+rdd3.partitions.size)

    rdd3.saveAsTextFile("output/tmp/coalesce")

    /**
     * Partición y repartición de DataFrame
     *
     */

    /**
     * A diferencia de RDD, no puede especificar la partición / paralelismo al crear DataFrame .
     * DataFrame o Dataset de forma predeterminada utiliza los métodos especificados
     * en la Sección 1 para determinar la partición predeterminada y
     * divide los datos para el paralelismo.
     */
    val df = spark.range(0,20)
    println(df.rdd.partitions.length)

    df.write.mode(SaveMode.Overwrite)csv("output/partition.csv")

    val df2 = df.repartition(6)
    println(df2.rdd.partitions.length)

    val df3 = df.coalesce(2)
    println(df3.rdd.partitions.length)

    /**
     * Repaticion aleatoria
     */
    val df4 = df.groupBy("id").count()
    println(df4.rdd.getNumPartitions)








  }

}
