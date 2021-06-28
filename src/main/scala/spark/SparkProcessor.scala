package spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import scala.util.Random

class SparkProcessor {
  private val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Test.com")
    .config("spark.es.nodes", "localhost")
    .config("spark.es.port", "9200")
    .getOrCreate()

  def getDataframe(path: String): DataFrame = {
    spark.sqlContext.read.json(path)
  }

  def getTypeDf(df: DataFrame, typeName: String, columnNames: Array[String]): DataFrame = {
    val columns = columnNames.map(name => df.col(name))

    val typeDf = df
      .filter(col("_table") === typeName)
      .select(columns: _*)

    val renamedColumns = typeDf.columns.map(c => typeDf(c).as(s"${typeName}_$c"))
    typeDf.select(renamedColumns: _*)
  }

  def transferToEs(df: DataFrame, indexName: String): Unit = {
    val batchSizeInMB = 4
    val batchEntries = 10000
    val batchRetryCount = 3
    val batchWriteRetryWait = 10

    val esConfig = Map(
      "es.resource" -> indexName,
      "es.batch.size.bytes" -> (batchSizeInMB * 1024 * 1024).toString,
      "es.batch.size.entries" -> batchEntries.toString,
      "es.batch.write.retry.count" -> batchRetryCount.toString,
      "es.batch.write.retry.wait" -> batchWriteRetryWait.toString,
      "es.batch.write.refresh" -> "false"
    )

    df.saveToEs(esConfig)
  }
}

object Runner {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val processor = new SparkProcessor()

    val df = processor.getDataframe(path)

    val studyDf = processor.getTypeDf(
      df, "study",
      Array("_id", "country", "type")
    )

    val patientDf = processor.getTypeDf(
      df, "patient",
      Array("_id", "_parentId", "age", "sex")
    )

    val sampleDf = processor.getTypeDf(
      df, "sample",
      Array("_id", "_parentId", "type", "organ")
    )

    val fullPathPatientDf = patientDf.join(
      studyDf,
      col("study__id") === col("patient__parentId")
    )

    val fullPathSampleDf = sampleDf.join(
      fullPathPatientDf,
      col("patient__id") === col("sample__parentId")
    )

    val result = fullPathSampleDf
      .drop("sample__parentId", "patient__parentId")

    //result.explain()
    processor.transferToEs(result, f"faceted-rd-index-${Random.nextInt()}")
  }
}
