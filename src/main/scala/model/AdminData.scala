package model

import org.apache.spark.sql.Dataset

case class AdminData (vat: Dataset[Vat], paye: Dataset[Paye])