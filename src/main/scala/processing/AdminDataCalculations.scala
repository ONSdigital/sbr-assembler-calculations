package processing

import common.config.ONSConfiguration
import dao.hbase.HBaseDao
import dao.hbase.HFileUtils.generateErn
import input.{LoadPaye, LoadVat}
import model.{Paye, Vat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import uk.gov.ons.registers.methods.{PayeCalculator, VatCalculator}
import util.AppParams
import spark._

object AdminDataCalculations extends PayeCalculator with VatCalculator with Serializable {

  val vatF: SparkSession => Dataset[Vat] = LoadVat.loadInput(ONSConfiguration("app.vatFilePath"))
  val payeF: SparkSession => Dataset[Paye] = LoadPaye.loadInput(ONSConfiguration("app.payeFilePath"))

  def calculate(implicit spark: SparkSession): DataFrame = {

    val vat = vatF(spark).cache()
    val paye = payeF(spark).cache()
    val allLinksLeusDF = getAllLinksLUsDF().cache()

    val payeCalculated: DataFrame = calculatePAYE(allLinksLeusDF, paye.toDF())

    calculateVAT(allLinksLeusDF, payeCalculated, vat.toDF())
  }

  def getAllLinksLUsDF()(implicit spark: SparkSession): DataFrame = {

    val incomingBiDataDF: DataFrame = getIncomingBiData

    val existingLinksLeusDF: DataFrame = getExistingLinksLeusDF

    val joinedLUs = incomingBiDataDF.join(
      existingLinksLeusDF.select("ubrn", "ern"),
      Seq("ubrn"), "left_outer")

    getAllLUs(joinedLUs)
  }

  def getAllLUs(joinedLUs: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val rows = joinedLUs.rdd.map {
      row => {
        val ern = if (row.isNull("ern")) generateErn(row) else row.getAs[String]("ern")

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
  def getExistingLinksLeusDF()(implicit spark: SparkSession): DataFrame = {
    val ruHFileRowRdd: RDD[Row] = HBaseDao.readTable(HBaseDao.rusTableName).map(_.toRuRow)
    spark.createDataFrame(ruHFileRowRdd, ruRowSchema)
  }

  def getIncomingBiData()(implicit spark: SparkSession): DataFrame = {
    val parquetDF = spark.read.parquet(AppParams.PATH_TO_PARQUET)

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
          row.getStringValueOrDefault("PostCode", AppParams.DEFAULT_POSTCODE),
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
