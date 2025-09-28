package link.rdcn.struct

import link.rdcn.util.CodecUtils
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import scala.collection.JavaConverters._

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/8 14:22
 * @Modified By:
 */
case class ArrowFlightStreamWriter(stream: Iterator[Row]) {

  def process(root: VectorSchemaRoot, batchSize: Int): Iterator[ArrowRecordBatch] = {
    stream.grouped(batchSize).map(rows => createDummyBatch(root, rows))
  }

  private def createDummyBatch(arrowRoot: VectorSchemaRoot, rows: Seq[Row]): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    val fieldVectors = arrowRoot.getFieldVectors.asScala
    var i = 0
    rows.foreach(row => {
      var j = 0
      fieldVectors.foreach(vec => {
        val value = row.get(j)
        value match {
          case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
          case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
          case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
          case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
          case v: java.math.BigDecimal => vec.asInstanceOf[VarCharVector].setSafe(i, v.toString.getBytes("UTF-8"))
          case v: String =>
            val bytes = v.getBytes("UTF-8")
            vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
          case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
          case v: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(i, v)
          case null => vec.setNull(i)
          case v: DFRef =>
            val bytes = v.url.getBytes("UTF-8")
            vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
          case v: Blob =>
            val blobId = BlobRegistry.register(v)
            val bytes = CodecUtils.encodeString(blobId)
            vec.asInstanceOf[VarBinaryVector].setSafe(i, bytes)
          case _ => throw new UnsupportedOperationException("Type not supported")
        }
        j += 1
      })
      i += 1
    })
    arrowRoot.setRowCount(rows.length)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }
}
