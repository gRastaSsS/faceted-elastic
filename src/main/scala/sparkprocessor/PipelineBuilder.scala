package sparkprocessor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import parser.{ModelTree, Node, RootProperty, SuccessorProperties, Transformer}

class PipelineBuilder(private val spark: SparkProcessor) {
  private def columnPrefix(col: String, prefix: String) = s"${prefix}_$col"

  def build(model: ModelTree, trans: Transformer): DataFrame = {
    val df = spark.mainDataframe

    //buildRootProperties(typeDfs, trans.root, trans.rootFacts.hasProperties)
    spark.mainDataframe
  }

  private def buildRootProperties(model: ModelTree, rootDf: DataFrame, root: Node, properties: Seq[RootProperty]): DataFrame = {
    val columns = (Seq(model.idField, model.parentIdField) ++: properties.map(p => p.name))
      .map(name => col(columnPrefix(name, root.name)))

    rootDf.select(columns: _*)
  }

  private def buildSuccessorProperties(model: ModelTree,
                                       dfs: Map[String, DataFrame],
                                       root: String,
                                       properties: Seq[SuccessorProperties]): DataFrame = {



    dfs(root)
  }
}
