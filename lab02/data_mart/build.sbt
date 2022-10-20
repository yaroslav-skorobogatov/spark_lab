ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.6.2"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"