package input

import org.apache.spark.sql.{Dataset, SparkSession}

trait LoadInput[T] extends Serializable {

  def loadInput (path: String) (implicit spark: SparkSession) : Dataset[T]

}
