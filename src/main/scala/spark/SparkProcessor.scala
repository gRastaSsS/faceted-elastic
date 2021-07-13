package spark

import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import parser.{ModelTree, RootFacts, RootProperty, SuccessorProperties, Transformer}

import scala.util.Random

class SparkProcessor(path: String) {
  private val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Test.com")
    .config("spark.es.nodes", "localhost")
    .config("spark.es.port", "9200")
    .getOrCreate()

  def getDataframe(path: String): DataFrame = {
    spark.sqlContext.read.json(path)
  }

  def idField = "_id"

  def parentIdField = "_parentId"

  def typeField = "_table"

  def mainDataframe: DataFrame = getDataframe(path)

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
  def globalJoin(path: String): Unit = {
    val processor = new SparkProcessor(path)

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

  def groupByExample(path: String): Unit = {
    val processor = new SparkProcessor(path)

    val df = processor.mainDataframe

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

    val result = fullPathPatientDf
      .groupBy("study__id", "patient_age")
      .agg(count("patient__id").as("patients_per_age"))

    processor.transferToEs(result, f"faceted-rd-index-${Random.nextInt()}")
  }

  def runTest(path: String): Unit = {
    val spark = new SparkProcessor(path)
    val pipelineBuilder = new PipelineBuilder(spark)

    val df = pipelineBuilder.build(
      ModelTree(Seq("study" -> "patient", "patient" -> "sample")),
      Transformer(
        "Test", "elasticsearch", "study",
        RootFacts(
          hasProperties = Seq(RootProperty("country")),
          hasSuccessorsWithProperties = Seq(
            SuccessorProperties("sample", Seq("type", "organ"))
          )
        )
      )
    )

    //spark.transferToEs(df, f"faceted-rd-index-${Random.nextInt()}")

    df.write
      .json("/Users/vladislav.gerasimov/IdeaProjects/faceted-elastic/src/test/data/output.json")
  }

  def main(args: Array[String]): Unit = {
    runTest(args(0))
  }
}
