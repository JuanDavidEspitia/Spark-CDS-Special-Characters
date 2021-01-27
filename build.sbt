import sbt.Keys.libraryDependencies
name := "Spark-CDS-Special-Characters"
crossScalaVersions := Seq("2.11.12", "2.12.10")
version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.3" % Provided
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3"