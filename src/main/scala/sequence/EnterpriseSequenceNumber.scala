package sequence

import common.sequence.SequenceGenerator
import common.config.ONSConfiguration

class EnterpriseSequenceNumber {

  /**
    * def fromHost(hostName: String = "localhost:2181", resultFormat: String = "11%07d",
    * path: String = "/ids/enterprise/id", sessionTimeoutSec: Int = 5,
    * connectionTimeoutSec: Int = 5)
    */

  val hostName = ONSConfiguration("app.zookeeper.host")
  val resultFormat = ONSConfiguration("app.zookeeper.resultFormat")
  val path = ONSConfiguration("app.zookeeper.path")
  val sessionTimeout = ONSConfiguration("app.zookeeper.sessionTimeoutSec")
  val connectionTimeout = ONSConfiguration("app.zookeeper.connectionTimeoutSec")

  val service: SequenceGenerator =
    SequenceGenerator.fromHost(hostName = hostName, sessionTimeoutSec = 1, connectionTimeoutSec = 1)

  def nextSequence: Long = service.nextSequence.toLong
  def nextSequence(batchSize: Int): (String, String) = service.nextSequence(batchSize)

}
