/**
 * Copyright (C) 2014 MediaMath <http://www.mediamath.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author daikeshi
 * @author themodernlife
 */

import java.util.{HashMap => JMap}

import com.twitter.algebird.HLL
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, HTableInterface, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._


class CoprocessorClient(val hConf: Configuration) {
  private def makeScan(table: String, query: GroupByQuery) = {
    val campaign = query.campaign
    val startDate = query.startDate
    val endDate = query.endDate
    val (startRow, stopRow) = (startDate, endDate) match {
      case (Some(start), Some(end)) ⇒ (s"$campaign:$start", s"$campaign:$end")
      case (Some(start), _) ⇒ (s"$campaign:$start", s"${campaign.toLong+1}")
      case _ ⇒ (s"$campaign", s"${campaign.toLong+1}")
    }

    val tableInfo = TableController(table)
    val scan = new Scan().setMaxVersions(1)
    query.metrics.foreach { metric ⇒ scan.addColumn(Bytes.toBytes(tableInfo.family), Bytes.toBytes(metric))}
    scan.setStartRow(Bytes.toBytes(startRow))
    scan.setStopRow(Bytes.toBytes(stopRow))
    scan.setFilter(new PrefixFilter(Bytes.toBytes(campaign)))
    scan
  }

  def groupBySum(query: GroupByQuery): JMap[String, JMap[String, Any]] = {
    val scan = makeScan(query.table, query)
    val call = GroupByMonoidSumCall(query, scan)
    val callback = GroupByMonoidSumCallback()

    var hTable: HTableInterface = null
    try {
      // get connection and get table
      hTable = new HTable(hConf, query.table)
      println("calling coprocessor")
      hTable.coprocessorExec(classOf[GroupByMonoidSumProtocol], null, null, call, callback)
    } finally {
      if (hTable != null) {
        hTable.close()
      }
    }
    callback.result
  }
}

object DemoApp extends App {
  val arguments = Args(args)
  val campaign = arguments.required("campaign")
  val startDate = arguments.optional("start-date")
  val endDate = arguments.optional("end-date")
  val dimensions = arguments.list("dimensions")
  val metrics = arguments.list("metrics")

  val hbaseConf = HBaseConfiguration.create
  hbaseConf.set("hbase.zookeeper.quorum", sys.env.getOrElse("ZOOKEEPER_QUORUM", "localhost"))
  hbaseConf.set("hbase.zookeeper.property.clientPort", sys.env.getOrElse("ZOOKEPPER_CLIENTPORT", "2181"))
  hbaseConf.setLong("hbase.rpc.timeout", 3000000L)

  val client = new CoprocessorClient(hbaseConf)
  val table = "mobile-device"
  val query = GroupByQuery(table, campaign, startDate, endDate, dimensions, metrics)
  val results = client.groupBySum(query)

  for ((k, cv) ← results) {
    print(s"$k: ")
    for ((c, v) ← cv) {
      val t = v match {
        case _: HLL ⇒ v.asInstanceOf[HLL].estimatedSize
        case _ ⇒ v
      }
      print(s"$c=$t, ")
    }
    print("\n")
  }
}
