package dao.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HBaseConnectionManager {

  val conf: Configuration = HBaseConfiguration.create()

  def withHbaseConnection(action: Connection => Unit): Unit = {

    implicit val hbConnection: Connection = ConnectionFactory.createConnection(conf)

    action(hbConnection)
    hbConnection.close()
  }

}