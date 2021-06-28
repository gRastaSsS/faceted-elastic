package spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import parser.{ModelTree, RootProperty, SuccessorProperties, Transformer}

class PipelineBuilder(private val spark: SparkProcessor) {
  private def idField = spark.idField
  private def parentIdField = spark.parentIdField
  private def typeField = spark.typeField

  private def columnPrefix(col: String, prefix: String) = s"${prefix}_$col"

  def build(model: ModelTree, trans: Transformer): DataFrame = {
    val df = spark.mainDataframe

    val typeDfs = model.transitions
      .map(t => t._1)
      .map(typeName => typeName -> typeDf(df, typeName))
      .toMap

    buildRootProperties(typeDfs, trans.root, trans.rootFacts.hasProperties)
  }

  private def buildRootProperties(dfs: Map[String, DataFrame], root: String, properties: Seq[RootProperty]): DataFrame = {
    val columns = (Seq(idField, parentIdField) ++: properties.map(p => p.name))
      .map(name => col(columnPrefix(name, root)))

    dfs(root).select(columns: _*)
  }

  private def typeDf(df: DataFrame, typeName: String): DataFrame = {
    val typeDf = df.filter(col(typeField) === typeName)
    val renamedColumns = typeDf.columns.map(c => typeDf(c).as(columnPrefix(c, typeName)))
    typeDf.select(renamedColumns: _*)
  }
}
