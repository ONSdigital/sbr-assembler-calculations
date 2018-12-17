package dao.hive

import util.AppParams.{HiveDBName, HiveTableName, HiveShortTableName}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HiveDao(implicit spark: SparkSession) {

  def getRegions: DataFrame =
    spark.sql(s"select postcode, gor as region from $HiveDBName.$HiveTableName")

  def getRegionsShort: DataFrame =
    spark.sql(s"select postcodeout,gor as region from $HiveDBName.$HiveShortTableName")

}