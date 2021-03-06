package dao.hbase

import model.domain.HFileRow
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Admin, Connection, Scan, Table}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{PrefixFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import util.AppParams

object HBaseDao extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def readTable(tableName: String)
               (implicit spark: SparkSession): RDD[HFileRow] = {

    val config = AppParams.hbaseConfiguration
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    val res = readKvsFromHBase
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def loadHFiles(implicit connection: Connection): Unit = {
    loadLinksHFile
    loadEnterprisesHFile
    loadLousHFile
  }

  def truncateTables(implicit connection: Connection): Unit = {
    truncateLinksTable
    truncateEntsTable
    truncateLousTable
    truncateLeusTable
    truncateRusTable
  }

  def truncateTable(tableName: String)(implicit connection: Connection): Unit =
    wrapTransaction(tableName) { (table, admin) =>
      admin.disableTable(table.getName)
      admin.truncateTable(table.getName, true)
    }

  def truncateLinksTable(implicit connection: Connection): Unit = truncateTable(linksTableName)

  def truncateEntsTable(implicit connection: Connection): Unit = truncateTable(entsTableName)

  def truncateLousTable(implicit connection: Connection): Unit = truncateTable(lousTableName)

  def truncateLeusTable(implicit connection: Connection): Unit = truncateTable(leusTableName)

  def truncateRusTable(implicit connection: Connection): Unit = truncateTable(rusTableName)

  def readDeleteData(regex: String)(implicit spark: SparkSession): Unit = {
    val data: RDD[HFileRow] = readLinksWithKeyFilter(regex)
    val rows: Array[HFileRow] = data.take(5)
    rows.map(_.toString).foreach(row => print(
      "=" * 10 + row + '\n' + "=" * 10
    ))
  }

  def readLinksWithKeyFilter(regex: String)
                            (implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(linksTableName, regex)
  }

  def readLinksWithKeyPrefixFilter(prefix: String)
                                  (implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithPrefixKeyFilter(linksTableName, prefix)
  }

  def readLouWithKeyFilter(regex: String)
                          (implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(lousTableName, regex)
  }

  def readEnterprisesWithKeyFilter(regex: String)
                                  (implicit spark: SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(entsTableName, regex)
  }

  def readTableWithPrefixKeyFilter(tableName: String, regex: String)
                                  (implicit spark: SparkSession): RDD[HFileRow] = {
    withKeyPrefixScanner(regex, tableName) {
      readKvsFromHBase
    }
  }

  def readTableWithKeyFilter(tableName: String, regex: String)
                            (implicit spark: SparkSession): RDD[HFileRow] = {
    withScanner(regex, tableName) {
      readKvsFromHBase
    }
  }

  def loadRefreshLinksHFile(implicit connection: Connection): Unit =
    wrapTransaction(linksTableName) { (table, admin) =>

      val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
      val regionLocator = connection.getRegionLocator(table.getName)
      bulkLoader.doBulkLoad(new Path(AppParams.PathToLinksHfile), admin, table, regionLocator)
    }

  def loadLinksHFile(implicit connection: Connection): Unit =
    wrapTransaction(linksTableName ){ (table, admin) =>
      val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
      val regionLocator = connection.getRegionLocator(table.getName)
      bulkLoader.doBulkLoad(new Path(AppParams.PathToLinksHfile), admin, table, regionLocator)
    }

  def loadEnterprisesHFile(implicit connection: Connection): Unit =
    wrapTransaction(entsTableName) { (table, admin) =>
      val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
      val regionLocator = connection.getRegionLocator(table.getName)
      bulkLoader.doBulkLoad(new Path(AppParams.PathToEnterpriseHFile), admin, table, regionLocator)
    }

  def loadLousHFile(implicit connection: Connection): Unit =
    wrapTransaction(lousTableName) { (table, admin) =>
      val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
      val regionLocator = connection.getRegionLocator(table.getName)
      bulkLoader.doBulkLoad(new Path(AppParams.PathToLocalUnitsHFile), admin, table, regionLocator)
    }

  def loadLeusHFile(implicit connection: Connection): Unit =
    wrapTransaction(leusTableName) { (table, admin) =>
      val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
      val regionLocator = connection.getRegionLocator(table.getName)
      bulkLoader.doBulkLoad(new Path(AppParams.PathToLegalUnitsHFile), admin, table, regionLocator)
    }

  def loadRusHFile(implicit connection: Connection): Unit =
    wrapTransaction(rusTableName) { (table, admin) =>
      val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
      val regionLocator = connection.getRegionLocator(table.getName)
      bulkLoader.doBulkLoad(new Path(AppParams.PathToReportingUnitsHFile), admin, table, regionLocator)
    }

  private def wrapTransaction(fullTableName: String)(action: (Table, Admin) => Unit)(implicit connection: Connection) {
    val tn = TableName.valueOf(fullTableName)
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table, admin)
    table.close()
  }

  private def wrapReadTransaction(tableName: String)(action: String => RDD[HFileRow])(implicit connection: Connection): RDD[HFileRow] = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val admin = connection.getAdmin
    setJob(table)
    val res = action(tableName)
    table.close()
    res
  }

  private def setJob(table: Table)(implicit connection: Connection) {
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
    //HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(table.getName))
  }

  def withKeyPrefixScanner(prefix: String, tableName: String)
                          (getResult: => RDD[HFileRow]): RDD[HFileRow] = {

    val config = AppParams.hbaseConfiguration
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setPrefixScanner(prefix)
    val res = getResult
    unsetPrefixScanner()
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def withScanner(regex: String, tableName: String)
                 (getResult: => RDD[HFileRow]): RDD[HFileRow] = {

    val config = AppParams.hbaseConfiguration
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setScanner(regex)
    val res = getResult
    unsetScanner()
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def readKvsFromHBase()(implicit spark: SparkSession): RDD[HFileRow] = {
    spark.sparkContext.newAPIHadoopRDD(
      AppParams.hbaseConfiguration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(row => HFileRow(row._2))
  }

  def copyExistingRecordsToHFiles(dirName: String = "existing")(implicit spark: SparkSession): Unit = {
    def buildPath(path: String) = {
      val dirs = path.split("/")
      val updatedDirs = (dirs.init :+ dirName) :+ dirs.last
      val res = updatedDirs.mkString("/")
      res
    }

    val prevTimePeriod = {
      (AppParams.TimePeriod.toInt - 1).toString
    }

    val ents: RDD[HFileRow] = HBaseDao.readEnterprisesWithKeyFilter(s"~$prevTimePeriod")
    val links: RDD[HFileRow] = HBaseDao.readLinksWithKeyFilter(s"~$prevTimePeriod")
    val lous: RDD[HFileRow] = HBaseDao.readLouWithKeyFilter(s".*~$prevTimePeriod~*.")

    ents.flatMap(_.toHFileCellRow(AppParams.HBaseEnterpriseColumnFactory)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(buildPath(AppParams.PathToEnterpriseHFile), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2])

    links.flatMap(_.toHFileCellRow(AppParams.HBaseLinksColumnFactory)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(buildPath(AppParams.PathToLinksHfile), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2])

    lous.flatMap(_.toHFileCellRow(AppParams.HBaseLocalUnitsColumnFactory)).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(buildPath(AppParams.PathToLocalUnitsHFile), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2])
  }

  private def unsetScanner(): Unit = AppParams.hbaseConfiguration.unset(TableInputFormat.SCAN)

  private def setScanner(regex: String): Unit = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    val scan = new Scan()
    scan.setFilter(filter)

    val scanStr = convertScanToString(scan)

    AppParams.hbaseConfiguration.set(TableInputFormat.SCAN, scanStr)
  }

  private def setPrefixScanner(prefix: String): Unit = {

    val prefixFilter = new PrefixFilter(prefix.getBytes)

    val scan: Scan = new Scan()
    scan.setFilter(prefixFilter)

    val scanStr = convertScanToString(scan)
    AppParams.hbaseConfiguration.set(TableInputFormat.SCAN, scanStr)
  }

  private def convertScanToString(scan: Scan): String = {
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  private def unsetPrefixScanner(): Unit = AppParams.hbaseConfiguration.unset(TableInputFormat.SCAN)

  def linksTableName: String =
    s"${AppParams.HBaseLinksTableNamespace}:${AppParams.HBaseLinksTableName}_${AppParams.TimePeriod}"

  def leusTableName: String = s"${AppParams.HBaseLegalUnitsNamespace}:${AppParams.HBaseLegalUnitsTableName}_${AppParams.TimePeriod}"

  def lousTableName: String = s"${AppParams.HBaseLocalUnitsNamespace}:${AppParams.HBaseLocalUnitsTableName}_${AppParams.TimePeriod}"

  def rusTableName: String = s"${AppParams.HBaseReportingUnitsNamespace}:${AppParams.HBaseReportingUnitsTableName}_${AppParams.TimePeriod}"

  def entsTableName: String = s"${AppParams.HBaseEnterpriseTableNamespace}:${AppParams.HBaseEnterpriseTableName}_${AppParams.TimePeriod}"

}
