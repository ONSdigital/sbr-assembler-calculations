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

  lazy val TimePeriod: String = ONSConfiguration("app.timePeriod")

  lazy val HBaseLinksTableNamespace: String = ONSConfiguration("app.links.namespace")
  lazy val HBaseLinksTableName: String = ONSConfiguration("app.links.tableName")
  lazy val HBaseLinksColumnFactory: String = ONSConfiguration("app.links.columnFamily")

  lazy val HBaseLegalUnitsNamespace: String = ONSConfiguration("app.legal.namespace")
  lazy val HBaseLegalUnitsTableName: String = ONSConfiguration("app.legal.tableName")
  lazy val HBaseLegalUnitsColumnFactory: String = ONSConfiguration("app.legal.columnFamily")

  lazy val HBaseLocalUnitsNamespace: String = ONSConfiguration("app.local.namespace")
  lazy val HBaseLocalUnitsTableName: String = ONSConfiguration("app.local.tableName")
  lazy val HBaseLocalUnitsColumnFactory: String = ONSConfiguration("app.local.columnFamily")

  lazy val HBaseReportingUnitsNamespace: String = ONSConfiguration("app.reporting.namespace")
  lazy val HBaseReportingUnitsTableName: String = ONSConfiguration("app.reporting.tableName")

  lazy val HBaseEnterpriseTableNamespace: String = ONSConfiguration("app.enterprise.namespace")
  lazy val HBaseEnterpriseTableName: String = ONSConfiguration("app.enterprise.tableName")
  lazy val HBaseEnterpriseColumnFactory: String = ONSConfiguration("app.enterprise.columnFamily")

  lazy val PathToLinksHfile: String = ONSConfiguration("app.links.filePath")
  lazy val PathToEnterpriseHFile: String = ONSConfiguration("app.enterprise.filePath")
  lazy val PathToLocalUnitsHFile: String = ONSConfiguration("app.local.filePath")
  lazy val PathToLegalUnitsHFile: String = ONSConfiguration("app.legal.filePath")
  lazy val PathToReportingUnitsHFile: String = ONSConfiguration("app.reporting.filePath")

  lazy val PathToParquet: String = ONSConfiguration("app.parquetFilePath")

  lazy val DefaultPRN = "0"
  lazy val DefaultWorkingProps = "0"
  lazy val DefaultRegion = ""
  lazy val DefaultPostCode = "XZ9 9XX"

  lazy val PathToGeo: String = ONSConfiguration("app.geo.pathToGeo")
  lazy val PathToGeoShort: String = ONSConfiguration("app.geo.pathToGeoShort")

  lazy val HiveDBName: String = ONSConfiguration("app.hive.hiveDBName")
  lazy val HiveTableName: String = ONSConfiguration("app.hive.hiveTablename")
  lazy val HiveShortTableName: String = ONSConfiguration("app.hive.hiveShortTablename")

  val newLeusViewName = "NEWLEUS"

  val ZookeeperHost = ONSConfiguration(OptionNames.ZookeeperHost)
  val ZookeeperFormat = ONSConfiguration("app.zookeeper.resultFormat")
  val ZookeeperPath = ONSConfiguration("app.zookeeper.path")
  val ZookeeperSessionTimeout = ONSConfiguration("app.zookeeper.sessionTimeoutSec")
  val ZookeeperConnectionTimeout = ONSConfiguration("app.zookeeper.connectionTimeoutSec")
}
