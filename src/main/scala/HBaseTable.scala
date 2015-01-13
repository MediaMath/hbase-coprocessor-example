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

import com.twitter.algebird.HyperLogLog
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hbase.util.Bytes

abstract class HBaseTable {
  val name: String
  val family: String
  val rowKeyFieldIndexMap: Map[String, Int]
}

object MobileDevice extends HBaseTable {
  val name = "mobile-device"
  val family = "stats"
  val rowKeyFieldIndexMap: Map[String, Int] = Map("campaign" → 0, "date" → 1, "device" → 2, "deviceOs" → 3)
}

case class MobileDeviceRecord(campaign: String, date: String, device: String, deviceOS: String, impressions: Long, hllHash: String) {
  private val kryo = KryoEnv.kryo
  private val hll = HyperLogLog.fromBytes(Base64.decodeBase64(hllHash))

  private def makeRowKeyString(parts: String*) = parts.toList.mkString(":")

  def rowKey: Array[Byte] = Bytes.toBytes(makeRowKeyString(campaign, date, device, deviceOS))

  // returns a list of (family, column, value)
  def colVals: List[(Array[Byte], Array[Byte], Array[Byte])] = List(
    (Bytes.toBytes("stats"), Bytes.toBytes("impressions"), kryo.toBytesWithClass(impressions)),
    (Bytes.toBytes("stats"), Bytes.toBytes("uniques"), kryo.toBytesWithClass(hll))
  )
}

object TableController {
  val tables = Map("mobile-device" → MobileDevice)
  def apply(name: String) = tables(name)
}
