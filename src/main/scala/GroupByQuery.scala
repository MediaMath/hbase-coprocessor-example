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

import org.apache.hadoop.hbase.util.Bytes

case class GroupByQuery(table: String, campaign: String, startDate: Option[String],
                        endDate: Option[String], dimensions: List[String], metrics: List[String]) {
  def groupByFromBytes(rowKey: Array[Byte]): String = {
    val rowKeyParts = Bytes.toString(rowKey).split(":")
    val indices = dimensions.map(x ⇒ TableController(table).rowKeyFieldIndexMap(x))
    indices.map(i ⇒ rowKeyParts(i)).mkString(":")
  }

  def groupBy(rowKey: String): String = {
    val rowKeyParts = rowKey.split(":")
    val indices = dimensions.map(x ⇒ TableController(table).rowKeyFieldIndexMap(x))
    indices.map(i ⇒ rowKeyParts(i)).mkString(":")
  }
}
