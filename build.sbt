name := "faceted-elastic"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "3.1.2"
val mysqlDriverVersion = "8.0.25"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
)

libraryDependencies ++= Seq (
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.13.2"
)

libraryDependencies ++= Seq (
  "mysql" % "mysql-connector-java" % mysqlDriverVersion,
  "org.apache.kafka" % "kafka-clients" % "2.8.0"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test