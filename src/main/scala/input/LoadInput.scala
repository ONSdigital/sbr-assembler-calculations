package input

import org.apache.spark.sql.{Dataset, SparkSession}

trait LoadInput[T] extends Serializable {

  def loadInput (path: String) (spark: SparkSession) : Dataset[T]

}
