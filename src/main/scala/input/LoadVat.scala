package input

import model.Vat
import org.apache.spark.sql.{Dataset, SparkSession}

object LoadVat extends LoadInput[Vat] {

   def loadInput  (path: String) (spark: SparkSession) : Dataset[Vat] = {

    import spark.implicits._

    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .as[Vat]

  }

}
