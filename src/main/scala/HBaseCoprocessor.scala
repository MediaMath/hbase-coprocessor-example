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

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.coprocessor.{BaseEndpointCoprocessor, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

trait GroupByMonoidSumProtocol extends CoprocessorProtocol {
  def groupByMonoidSum(query: GroupByQuery, scan: Scan): JMap[String, JMap[String, Any]]
}

class GroupByMonoidSumCoprocessorEndpoint extends BaseEndpointCoprocessor with GroupByMonoidSumProtocol {
  private val kryo = KryoEnv.kryo
  private val monoids = Monoids

  override def groupByMonoidSum(query: GroupByQuery, scan: Scan): JMap[String, JMap[String, Any]] = {
    val region = getEnvironment.asInstanceOf[RegionCoprocessorEnvironment].getRegion
    println("get region")
    val scanner = region.getScanner(scan)
    println("get region scanner")
    val results = new java.util.ArrayList[KeyValue]
    val ret = new JMap[String, JMap[String, Any]]
    try {
      var hasMoreRows = false
      do {
        hasMoreRows = scanner.next(results)
        for (kv: KeyValue ← results) {
          val buffer = kv.getBuffer
          val row = Bytes.toString(kv.getBuffer, kv.getRowOffset, kv.getRowLength)
          val column = Bytes.toString(kv.getBuffer, kv.getQualifierOffset, kv.getQualifierLength)
          val gpKey = query.groupBy(row)
          val value = kryo.fromBytes(buffer.slice(kv.getValueOffset, kv.getValueOffset + kv.getValueLength)).asInstanceOf[Any]
          if (ret.containsKey(gpKey)) {
            val colVals = ret.get(gpKey)
            if (colVals.containsKey(column))
              colVals.put(column, monoids.plus(value, colVals.get(column)))
            else
              colVals.put(column, value)
          } else {
            val colVals = new JMap[String, Any]
            colVals.put(column, value)
            ret.put(gpKey, colVals)
          }
        }
        results.clear()
      } while (hasMoreRows)
    } finally {
      scanner.close()
    }
    ret
  }
}

case class GroupByMonoidSumCall(query: GroupByQuery, scan: Scan) extends Batch.Call[GroupByMonoidSumProtocol, JMap[String, JMap[String, Any]]] {
  override def call(instance: GroupByMonoidSumProtocol): JMap[String, JMap[String, Any]] = instance.groupByMonoidSum(query, scan)
}

case class GroupByMonoidSumCallback() extends Batch.Callback[JMap[String, JMap[String, Any]]] {
  private val monoids = Monoids
  private val aggregator = new JMap[String, JMap[String, Any]]

  override def update(region: Array[Byte], row: Array[Byte], nextResult: JMap[String, JMap[String, Any]]): Unit = {
    println("start final aggregation")
    for ((nxtKey, nxtColVals) ← nextResult) {
      if (aggregator.containsKey(nxtKey)) {
        val aggColVals = aggregator.get(nxtKey)
        for ((c, v) ← nxtColVals)
          aggColVals.put(c, monoids.plus(aggColVals.get(c), v))
      } else {
        aggregator.put(nxtKey, nxtColVals)
      }
    }
    println("finish final aggregation")
  }

  def result: JMap[String, JMap[String, Any]] = aggregator
}
