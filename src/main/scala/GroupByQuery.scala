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
