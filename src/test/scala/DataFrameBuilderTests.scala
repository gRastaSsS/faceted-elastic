import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import parser.{ConfigNode, ModelTree}
import sparkprocessor.DataFrameBuilder


class DataFrameBuilderTests extends FunSuite with BeforeAndAfterAll {
  var spark : SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  test("DataFrameBuilder.buildJoinedDfs") {
    val testFile = getClass.getResource("/test_data_1.json").toString
    val df = spark.read.json(testFile)

    val tree = ModelTree(Seq(
      ConfigNode("study", Seq("patient"), fields = Seq("country", "type")),
      ConfigNode("patient", Seq("sample"), fields = Seq("age", "sex")),
      ConfigNode("sample", Seq(), fields = Seq("organ", "type"))
    ))

    val builder = new DataFrameBuilder(tree)
    val joinDfs = builder.buildJoinedDfs(df)

    val studyDfResult = joinDfs("study").collectAsList()
    val patientDfResult = joinDfs("patient").collectAsList()
    val sampleDfResult = joinDfs("sample").collectAsList()

    assert(studyDfResult.size() === 1)
    assert(patientDfResult.size() === 1)
    assert(sampleDfResult.size() === 2)

    val studyDfRow = studyDfResult.get(0)
    assert(studyDfRow.getString(studyDfRow.fieldIndex("study_country")) === "Russia")
    assert(studyDfRow.getLong(studyDfRow.fieldIndex("study__id")) === 0)
  }
}
