package processing

import common.spark.SparkSessionManager.withSpark
import org.apache.hadoop.hbase.client.Connection
import dao.hbase.HBaseConnectionManager.withHbaseConnection
import org.apache.spark.sql.SparkSession
import pipeline.BuildInfo

object Calculations extends Serializable {

  def runPipeLine(): Unit = withSpark(BuildInfo.name, {

    implicit spark: SparkSession =>
      withHbaseConnection {
        implicit con: Connection => {

          AdminDataCalculations().clearTurnover()
          AdminDataCalculations().createUnitsHfiles()

        }
      }
  })

}
