package sparkprocessor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import parser.{ModelTree, Node}

class DataFrameBuilder(private val model: ModelTree) {
  private def columnPrefix(col: String, prefix: String) = s"${prefix}_$col"

  def buildJoinedDfs(df: DataFrame): Map[String, DataFrame] = {
    buildJoinedDfs(model.root, df, Option.empty)
  }

  private def buildJoinedDfs(node: Node, df: DataFrame, parent: Option[(Node, DataFrame)]): Map[String, DataFrame] = {
    val nodeDf: DataFrame = parent match {
      case Some((parentNode, parentDf)) => buildDf(df, node).join(
        parentDf,
        col(columnPrefix(model.idField, parentNode.name)) === col(columnPrefix(model.parentIdField, node.name))
      )
      case None => buildDf(df, node)
    }

    Map((node.name, nodeDf)) ++
      node.children.flatMap(child => buildJoinedDfs(child, df, Option.apply((node, nodeDf)))).toMap
  }

  private def buildDf(df: DataFrame, node: Node): DataFrame = {
    val typeDf = df.filter(col(model.typeField) === node.name)
    val allColumns = Seq(model.parentIdField, model.idField, model.typeField) ++ node.fields
    val renamedColumns = allColumns.map(c => typeDf(c).as(columnPrefix(c, node.name)))
    typeDf.select(renamedColumns: _*)
  }
}
