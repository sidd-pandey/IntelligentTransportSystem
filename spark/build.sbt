name := "spark"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"