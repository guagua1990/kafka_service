package com.liveramp.kafka_service

import org.apache.zookeeper.server.quorum.QuorumPeerMain

/**
 * Created by yjin on 2/13/15.
 */
object ScalaClient {

  def main(args: Array[String]) {
    val p = new QuorumPeerMain
    print("hello world")

    print(p)
  }
}
