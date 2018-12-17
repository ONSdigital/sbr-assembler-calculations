package input

import model.Paye
import org.apache.spark.sql.{Dataset, SparkSession}

object LoadPaye extends LoadInput[Paye] {

  override def loadInput(path: String) (implicit spark: SparkSession) : Dataset[Paye] = {

    import spark.implicits._

    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .as[Paye]

  }

}