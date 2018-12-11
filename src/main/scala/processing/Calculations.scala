package processing

import common.hbase.HBaseConnectionManager.withHbaseConnection
import common.spark.SparkSessionManager.withSpark
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import pipeline.BuildInfo

object Calculations extends Serializable {

  def runPipeLine(): Unit = withSpark(BuildInfo.name, {
    implicit spark: SparkSession =>
      withHbaseConnection {
        implicit con: Connection => {

          AdminDataCalculations.calculate

        }
      }
  })


}
