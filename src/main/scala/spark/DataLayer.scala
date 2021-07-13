package spark

import org.apache.spark.sql.DataFrame

case class DataLayer(idField: String, parentIdField: String, df: DataFrame)
