package util

import common.config.ONSConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object AppParams extends Serializable {

  val hbaseConfiguration: Configuration = HBaseConfiguration.create

  if (ONSConfiguration("hadoop.security.authentication") == "NOT_FOUND")
    hbaseConfiguration.set("hadoop.security.authentication", "kerberos")
  else
    hbaseConfiguration.addResource(ONSConfiguration("hadoop.security.authentication"))

  lazy val TIME_PERIOD: String = ONSConfiguration("app.timePeriod")

  lazy val HBASE_LINKS_TABLE_NAMESPACE: String = ONSConfiguration("app.links.namespace")
  lazy val HBASE_LINKS_TABLE_NAME: String = ONSConfiguration("app.links.tableName")
  lazy val HBASE_LINKS_COLUMN_FAMILY: String = ONSConfiguration("app.links.columnFamily")

  lazy val HBASE_LEGALUNITS_TABLE_NAMESPACE: String = ONSConfiguration("app.legal.namespace")
  lazy val HBASE_LEGALUNITS_TABLE_NAME: String = ONSConfiguration("app.legal.tableName")
  lazy val HBASE_LEGALUNITS_COLUMN_FAMILY: String = ONSConfiguration("app.legal.columnFamily")

  lazy val HBASE_LOCALUNITS_TABLE_NAMESPACE: String = ONSConfiguration("app.local.namespace")
  lazy val HBASE_LOCALUNITS_TABLE_NAME: String = ONSConfiguration("app.local.tableName")
  lazy val HBASE_LOCALUNITS_COLUMN_FAMILY: String = ONSConfiguration("app.local.columnFamily")

  lazy val HBASE_REPORTINGUNITS_TABLE_NAMESPACE: String = ONSConfiguration("app.reporting.namespace")
  lazy val HBASE_REPORTINGUNITS_TABLE_NAME: String = ONSConfiguration("app.reporting.tableName")

  lazy val HBASE_ENTERPRISE_TABLE_NAMESPACE: String = ONSConfiguration("app.enterprise.namespace")
  lazy val HBASE_ENTERPRISE_TABLE_NAME: String = ONSConfiguration("app.enterprise.tableName")
  lazy val HBASE_ENTERPRISE_COLUMN_FAMILY: String = ONSConfiguration("app.enterprise.columnFamily")


  lazy val PATH_TO_LINKS_HFILE: String = ONSConfiguration("app.links.filePath")
  lazy val PATH_TO_ENTERPRISE_HFILE: String = ONSConfiguration("app.enterprise.filePath")
  lazy val PATH_TO_LOCALUNITS_HFILE: String = ONSConfiguration("app.local.filePath")
  lazy val PATH_TO_LEGALUNITS_HFILE: String = ONSConfiguration("app.legal.filePath")
  lazy val PATH_TO_REPORTINGUNITS_HFILE: String = ONSConfiguration("app.reporting.filePath")

  lazy val PATH_TO_PARQUET: String = ONSConfiguration("app.parquetFilePath")

  lazy val DEFAULT_PRN = "0"
  lazy val DEFAULT_WORKING_PROPS = "0"
  lazy val DEFAULT_REGION = ""
  lazy val DEFAULT_POSTCODE = "XZ9 9XX"
}
