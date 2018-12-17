package processing

import HFile.HFileCell
import common.config.ONSConfiguration
import common.config.environment.Environment
import dao.hbase.{HBaseDao, HFileUtils}
import dao.hive.HiveDao
import input.{LoadPaye, LoadVat}
import model.{Paye, Vat}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spark._
import uk.gov.ons.registers.methods.{PayeCalculator, VatCalculator}
import util.AppParams
import org.apache.spark.sql.functions._

import scala.util.Random

case class AdminDataCalculations(implicit spark: SparkSession, con: Connection)
  extends PayeCalculator with VatCalculator with Serializable {

  def createUnitsHfiles(): Unit = {

    val regionsByPostcodeDF: DataFrame =
      if (Environment.isInCluster) HiveDao().getRegions.cache
      else spark.read.option("header", "true").csv(AppParams.PathToGeo)
        .select("pcds", "rgn").toDF("postcode", "region").cache

    val regionsByPostcodeShortDF: DataFrame =
      if (Environment.isInCluster) HiveDao().getRegionsShort.cache
      else spark.read.option("header", "true").csv(AppParams.PathToGeoShort)
        .select("pcds", "rgn").toDF("postcodeout", "region").cache

    regionsByPostcodeDF.collect()

    val allLinksLeusDF = getAllLinksLUsDF.cache
    val allEntsDF = getAllEntsCalculated(allLinksLeusDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache

    //    val allRusDF = getAllRus(allEntsDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache
    //
    //    val allLousDF = getAllLous(allRusDF, regionsByPostcodeDF, regionsByPostcodeShortDF).cache
    //
    //    val allLeusDF = getAllLeus(appconf, Configs.conf).cache()

    saveEnts(allEntsDF)
    //    saveRus(allRusDF, appconf)
    //    saveLous(allLousDF, appconf)
    //    saveLeus(allLeusDF, appconf)
    //    saveLinks(allLousDF, allRusDF, allLinksLeusDF, appconf)

    //    allLeusDF.unpersist()
    //    allLousDF.unpersist()
    //    allRusDF.unpersist()
    allEntsDF.unpersist()
    //    allLinksLeusDF.unpersist()
    regionsByPostcodeDF.unpersist()
  }

  def saveEnts(entsDF: DataFrame): Unit = {

    val hfileCells: RDD[(String, HFile.HFileCell)] = getEntHFileCells(entsDF)
    hfileCells.filter(_._2.value != null).sortBy(t => s"${t._2.key}${t._2.qualifier}")
      .map(rec => (new ImmutableBytesWritable(rec._1.getBytes()), rec._2.toKeyValue))
      .saveAsNewAPIHadoopFile(AppParams.PathToEnterpriseHFile, classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat2], AppParams.hbaseConfiguration)
  }

  def rowToEnt(row: Row): Seq[(String, HFileCell)] = {
    val ern = row.getStringOption("ern").get //must be there
    val prn = row.getStringOption("prn").get //must be there
    val workingProps = row.getStringOption("working_props").getOrElse("0") //must be there
    val employment = row.getStringOption("employment").getOrElse("0") //must be there
    val region = row.getStringOption("region").getOrElse("0") //must be there
    Seq(
      createEnterpriseCell(ern, "ern", ern),
      createEnterpriseCell(ern, "prn", prn),
      createEnterpriseCell(ern, "working_props", workingProps),
      createEnterpriseCell(ern, "employment", employment),
      createEnterpriseCell(ern, "region", region)
    ) ++
      Seq(
        row.getStringOption("entref").map(ref => createEnterpriseCell(ern, "entref", ref)),
        row.getStringOption("name").map(name => createEnterpriseCell(ern, "name", name)),
        row.getStringOption("trading_style").map(ls => createEnterpriseCell(ern, "trading_style", ls)),
        row.getStringOption("legal_status").map(ls => createEnterpriseCell(ern, "legal_status", ls)),
        row.getStringOption("address1").map(a1 => createEnterpriseCell(ern, "address1", a1)),
        row.getStringOption("address2").map(a2 => createEnterpriseCell(ern, "address2", a2)),
        row.getStringOption("address3") map (a3 => createEnterpriseCell(ern, "address3", a3)),
        row.getStringOption("address4").map(a4 => createEnterpriseCell(ern, "address4", a4)),
        row.getStringOption("address5") map (a5 => createEnterpriseCell(ern, "address5", a5)),
        row.getStringOption("postcode").map(pc => createEnterpriseCell(ern, "postcode", pc)),
        {
          val sic = row.getStringOption("sic07").getOrElse("")
          Some(createEnterpriseCell(ern, "sic07", sic))
        },
        row.getStringOption("paye_empees").map(employees => createEnterpriseCell(ern, "paye_empees", employees)),
        row.getStringOption("paye_jobs").map(jobs => createEnterpriseCell(ern, "paye_jobs", jobs)),
        row.getStringOption("app_turnover").map(apportion => createEnterpriseCell(ern, "app_turnover", apportion)),
        row.getStringOption("ent_turnover").map(total => createEnterpriseCell(ern, "ent_turnover", total)),
        row.getStringOption("cntd_turnover").map(contained => createEnterpriseCell(ern, "cntd_turnover", contained.toString)),
        row.getStringOption("std_turnover").map(standard => createEnterpriseCell(ern, "std_turnover", standard)),
        row.getStringOption("grp_turnover").map(group => createEnterpriseCell(ern, "grp_turnover", group))

      ).collect { case Some(v) => v }
  }

  def createEnterpriseCell(ern: String, column: String, value: String): (String, HFileCell) =
    createRecord(generateEntKey(ern), AppParams.HBaseEnterpriseColumnFactory, column, value)

  private def generateEntKey(ern: String): String = s"${ern.reverse}"

  private def createRecord(key: String, columnFamily: String, column: String, value: String) =
    key -> HFileCell(key, columnFamily, column, value)

  def getEntHFileCells(entsDF: DataFrame)(implicit spark: SparkSession): RDD[(String, HFile.HFileCell)] = {
    import spark.implicits._
    val res: RDD[(String, HFile.HFileCell)] = entsDF.map(row => rowToEnt(row)).flatMap(identity).rdd
    res
  }

  def calculateRegion(dfWithPostcode: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                     (implicit spark: SparkSession): Dataset[Row] = {

    //dfWithPostcode.withColumn("region",lit(""))
    val partitions = dfWithPostcode.rdd.getNumPartitions
    val step1DF = dfWithPostcode.drop("region")
    val step2DF = step1DF.join(regionsByPostcodeDF, Seq("postcode"), "left_outer")
    val step3DF = step2DF.select("*").where("region IS NULL")
    val partial = step2DF.select("*").where("region IS NOT NULL")
    val step4DF = step3DF.drop("region")
    val step5DF = step4DF.select(col("*"),
      trim(substring(col("postcode"), 0,
      col("postcode").toString().length - 4))
        .as("postcodeout"))
    val step6DF = step5DF.join(regionsByPostcodeShortDF, Seq("postcodeout"), "left_outer")
    val step7DF = step6DF.drop("postcodeout")
    val step8DF = step7DF.union(partial)
    val step9DF = step8DF.na.fill(AppParams.DefaultRegion, Seq("region"))
    val step10DF = step9DF.coalesce(partitions)
    step10DF
  }

  /**
    * requires working_props and paye_empees recalculated
    **/
  def calculateEmployment(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    df.createOrReplaceTempView("CALCULATEEMPLOYMENT")

    val sql =
      """ SELECT *,
                 CAST(
                      (CAST((CASE WHEN paye_empees is NULL THEN 0 ELSE paye_empees END) AS long) + CAST(working_props AS long))
                     AS string) AS employment
          FROM CALCULATEEMPLOYMENT
    """.stripMargin


    spark.sql(sql)
  }

  /**
    * expects df with fields 'legal_status', 'postcode', 'paye_empees', 'working_props'
    **/
  def calculateDynamicValues(df: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                            (implicit spark: SparkSession): Dataset[Row] = {
    val withWorkingProps = calculateWorkingProps(df)
    val withEmployment = calculateEmployment(withWorkingProps)
    calculateRegion(withEmployment, regionsByPostcodeDF, regionsByPostcodeShortDF)
  }

  def calculateWorkingProps(dfWithLegalStatus: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import dfWithLegalStatus.sqlContext.implicits.StringToColumn
    import org.apache.spark.sql.functions.udf

    def calculation = udf((legalStatus: String) => getWorkingPropsByLegalStatus(legalStatus))

    dfWithLegalStatus.withColumn("working_props", calculation($"legal_status"))
  }

  def getWorkingPropsByLegalStatus(legalStatus: String): String = legalStatus match {
    case "2" => "1"
    case "3" => "2"
    case _ => AppParams.DefaultWorkingProps
  }

  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    **/
  def createNewEntsWithCalculations(newLEUsCalculatedDF: DataFrame): DataFrame =
    spark.createDataFrame(

      newLEUsCalculatedDF.rdd.map(row => Row(
        row.getAs[String]("ern"),
        HFileUtils.generatePrn(row),
        row.getValueOrNull("entref"),
        row.getAs[String]("name"),
        null, //trading_style
        row.getValueOrEmptyStr("address1"),
        row.getAs[String]("address2"),
        row.getAs[String]("address3"),
        row.getAs[String]("address4"),
        row.getAs[String]("address5"),
        row.getAs[String]("postcode"),
        row.getValueOrEmptyStr("region"),
        row.getValueOrEmptyStr("industry_code"),
        row.getAs[String]("legal_status"),
        row.getValueOrNull("paye_empees"),
        row.getValueOrNull("paye_jobs"),
        row.getValueOrNull("cntd_turnover"),
        row.getValueOrNull("app_turnover"),
        row.getValueOrNull("std_turnover"),
        row.getValueOrNull("grp_turnover"),
        row.getValueOrNull("ent_turnover"),
        row.getStringOption("working_props").getOrElse("0"),
        row.getStringOption("employment").getOrElse("0")
      )
      ), completeEntSchema
    )

  def getAllEntsCalculated(allLinksLusDF: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame): Dataset[Row] = {

    val calculatedDF = calculate(allLinksLusDF).castAllToString()
    calculatedDF.cache()

    val existingEntDF = getExistingEntsDF

    val existingEntCalculatedDF: DataFrame = {
      val calculatedExistingEnt = existingEntDF.join(calculatedDF, Seq("ern"), "left_outer")
      val existingEntsWithRegionRecalculatedDF = calculateRegion(calculatedExistingEnt, regionsByPostcodeDF, regionsByPostcodeShortDF)
      val existingEntsWithEmploymentRecalculatedDF = calculateEmployment(existingEntsWithRegionRecalculatedDF)
      val withReorderedColumns: DataFrame = {
        val columns: Array[String] = completeEntSchema.fieldNames
        existingEntsWithEmploymentRecalculatedDF.select(columns.head, columns.tail: _*)
      }
      spark.createDataFrame(withReorderedColumns.rdd, completeEntSchema)
    }
    val newLEUsDF = allLinksLusDF.join(existingEntCalculatedDF.select(col("ern")), Seq("ern"), "left_anti")
    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"), "left_outer")

    val newLeusWithWorkingPropsAndRegionDF = calculateDynamicValues(newLEUsCalculatedDF, regionsByPostcodeDF, regionsByPostcodeShortDF)

    val newEntsCalculatedDF = spark.createDataFrame(createNewEntsWithCalculations(newLeusWithWorkingPropsAndRegionDF).rdd, completeEntSchema)
    val newLegalUnitsDF: DataFrame = getNewLeusDF(newLeusWithWorkingPropsAndRegionDF)
    newLegalUnitsDF.cache() //TODO: check if this is actually needed
    newLegalUnitsDF.createOrReplaceTempView(AppParams.newLeusViewName)

    val allEntsDF = existingEntCalculatedDF.union(newEntsCalculatedDF)

    calculatedDF.unpersist()
    allEntsDF
  }

  def getNewLeusDF(newLEUsCalculatedDF: DataFrame): DataFrame = {
    val newLegalUnitsDS: RDD[Row] = newLEUsCalculatedDF.rdd.map(row => new GenericRowWithSchema(Array(

      row.getAs[String]("ubrn"),
      row.getAs[String]("ern"),
      HFileUtils.generatePrn(row),
      row.getValueOrNull("crn"),
      row.getValueOrEmptyStr("name"),
      row.getValueOrNull("trading_style"), //will not be present
      row.getValueOrEmptyStr("address1"),
      row.getValueOrNull("address2"),
      row.getValueOrNull("address3"),
      row.getValueOrNull("address4"),
      row.getValueOrNull("address5"),
      row.getValueOrEmptyStr("postcode"),
      row.getValueOrEmptyStr("industry_code"),
      row.getValueOrNull("paye_jobs"),
      row.getValueOrNull("turnover"),
      row.getValueOrEmptyStr("legal_status"),
      row.getValueOrNull("trading_status"),
      row.getValueOrEmptyStr("birth_date"),
      row.getValueOrNull("death_date"),
      row.getValueOrNull("death_code"),
      row.getValueOrNull("uprn")
    ), leuRowSchema))

    spark.createDataFrame(newLegalUnitsDS, leuRowSchema)
  }

  def getExistingEntsDF: DataFrame = {
    val entHFileRowRdd: RDD[Row] = HBaseDao.readTable(HBaseDao.entsTableName).map(_.toEntRow)
    spark.createDataFrame(entHFileRowRdd, entRowSchema)
  }

  /**
    * To (re)calculate the PAYE and VAT we first need to ensure that existing values are removed from HBase
    */
  def clearTurnover(): Unit = {
    /*
    paye_jobs
    paye_empees
    cntd_turnover
    std_turnover
    grp_turnover
    app_turnover
    ent_turnover
     */

  }

  def calculate(unitsDF: DataFrame): DataFrame = {

    val vat: Dataset[Vat] = LoadVat.loadInput(ONSConfiguration("app.vatFilePath"))
    val paye: Dataset[Paye] = LoadPaye.loadInput(ONSConfiguration("app.payeFilePath"))

    val payeCalculated: DataFrame = calculatePAYE(unitsDF, paye.toDF)

    calculateVAT(unitsDF, payeCalculated, vat.toDF)
  }

  def getAllLinksLUsDF: DataFrame = {

    val incomingBiDataDF: DataFrame = getIncomingBiData
    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF
    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.select("ubrn", "ern"),
      Seq("ubrn"), "left_outer")

    getAllLUs(joinedLUs)
  }

  // Change Me
  def generateUniqueKey: String = "N" + Random.alphanumeric.take(17).mkString

  def getAllLUs(joinedLUs: DataFrame): DataFrame = {

    val rows = joinedLUs.rdd.map {
      row => {
        val ern =
          if (row.isNull("ern"))
            generateUniqueKey
          else
            row.getAs[String]("ern")

        Row(
          ern,
          row.getAs[String]("ubrn"),
          row.getAs[String]("name"),
          row.getAs[String]("industry_code"),
          row.getAs[String]("legal_status"),
          row.getValueOrEmptyStr("address1"),
          row.getAs[String]("address2"),
          row.getAs[String]("address3"),
          row.getAs[String]("address4"),
          row.getAs[String]("address5"),
          row.getValueOrEmptyStr("postcode"),
          row.getAs[String]("trading_status"),
          row.getAs[String]("turnover"),
          row.getAs[String]("uprn"),
          row.getAs[String]("crn"),
          row.getAs[Seq[String]]("payerefs"),
          row.getAs[Seq[String]]("vatrefs")
        )
      }
    }
    spark.createDataFrame(rows, biWithErnSchema)
  }

  /**
    * returns existing LEU~ links DF
    * fields:
    * ubrn, ern, CompanyNo, PayeRefs, VatRefs
    **/
  def getExistingLinksLeusDF: DataFrame = {
    val ruHFileRowRdd: RDD[Row] = HBaseDao.readTable(HBaseDao.rusTableName).map(_.toRuRow)
    spark.createDataFrame(ruHFileRowRdd, ruRowSchema)
  }

  def getIncomingBiData: DataFrame = {
    val parquetDF = spark.read.parquet(AppParams.PathToParquet)

    val rows = parquetDF.castAllToString().rdd.map {
      row => {

        Row(
          null,
          row.getAs[String]("id"),
          row.getAs[String]("BusinessName"),
          row.getAs[String]("IndustryCode"),
          row.getAs[String]("LegalStatus"),
          row.getValueOrEmptyStr("Address1"),
          row.getAs[String]("Address2"),
          row.getAs[String]("Address3"),
          row.getAs[String]("Address4"),
          row.getAs[String]("Address5"),
          row.getStringValueOrDefault("PostCode", AppParams.DefaultPostCode),
          row.getAs[String]("TradingStatus"),
          row.getAs[String]("Turnover"),
          row.getAs[String]("UPRN"),
          row.getAs[String]("CompanyNo"),
          row.getAs[Seq[String]]("PayeRefs"),
          row.getAs[Seq[String]]("VatRefs")
        )
      }
    }
    spark.createDataFrame(rows, biWithErnSchema)
  }

}
