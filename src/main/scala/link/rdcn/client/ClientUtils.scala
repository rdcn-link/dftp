package link.rdcn.client

import link.rdcn.struct.ValueType.{BinaryType, BlobType, BooleanType, DoubleType, FloatType, IntType, LongType, RefType, StringType}
import link.rdcn.struct.{ClosableIterator, Column, DFRef, DataFrame, DefaultDataFrame, Row, StructType, ValueType}
import link.rdcn.util.{CodecUtils, DataUtils}
import org.apache.arrow.flight.{PutResult, Result, SyncPutListener}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.{FloatingPointPrecision, Types}
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}

import java.io.ByteArrayInputStream
import java.util.Collections
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 15:28
 * @Modified By:
 */
object ClientUtils {

  def arrowSchemaToStructType(schema: org.apache.arrow.vector.types.pojo.Schema): StructType = {
    val columns = schema.getFields.asScala.map { field =>
      val colType = field.getType match {
        case t if t == Types.MinorType.INT.getType => ValueType.IntType
        case t if t == Types.MinorType.BIGINT.getType => ValueType.LongType
        case t if t == Types.MinorType.FLOAT4.getType => ValueType.FloatType
        case t if t == Types.MinorType.FLOAT8.getType => ValueType.DoubleType
        case t if t == Types.MinorType.VARCHAR.getType =>
          if (field.getMetadata.isEmpty) ValueType.StringType else ValueType.RefType
        case t if t == Types.MinorType.BIT.getType => ValueType.BooleanType
        case t if t == Types.MinorType.VARBINARY.getType => if (field.getMetadata.isEmpty)
          ValueType.BinaryType else ValueType.BlobType
        case _ => throw new UnsupportedOperationException(s"Unsupported Arrow type: ${field.getType}")
      }
      Column(field.getName, colType)
    }
    StructType(columns.toList)
  }

  def getVectorSchemaRootFromBytes(bytes: Array[Byte], allocator: BufferAllocator): VectorSchemaRoot = {
    val inputStream = new ByteArrayInputStream(bytes)
    val reader = new ArrowStreamReader(inputStream, allocator)
    reader.loadNextBatch()
    reader.getVectorSchemaRoot
  }

  def convertStructTypeToArrowSchema(structType: StructType): Schema = {
    val fields: List[Field] = structType.columns.map { column =>
      val arrowFieldType = column.colType match {
        case IntType =>
          new FieldType(column.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(column.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(column.nullable, ArrowType.Bool.INSTANCE, null)
        case BinaryType =>
          new FieldType(column.nullable, new ArrowType.Binary(), null)
        case RefType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "Url")
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null, metadata)
        case BlobType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "blob")
          new FieldType(column.nullable, new ArrowType.Binary(), null, metadata)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${column.colType}")
      }

      new Field(column.name, arrowFieldType, Collections.emptyList())
    }.toList

    new Schema(fields.asJava)
  }

  def parsePutListener(putListener: SyncPutListener): Option[Array[Byte]] = {
    val ack: PutResult = putListener.read()
    if (ack == null) {
      putListener.getResult()
      None
    } else {
      try {
        val metadataBuf = ack.getApplicationMetadata
        val bytes = new Array[Byte](metadataBuf.readableBytes().toInt)
        metadataBuf.readBytes(bytes)
        Some(bytes)
      } finally {
        ack.close()
      }
    }
  }

  def parseFlightActionResults(resultIterator: java.util.Iterator[Result], allocator: BufferAllocator): DataFrame = {
    val allRows = scala.collection.mutable.ArrayBuffer[Row]()
    var schema: StructType = StructType.empty
    while (resultIterator.hasNext) {
      val result = resultIterator.next()
      val vectorSchemaRootReceived = getVectorSchemaRootFromBytes(result.getBody, allocator)
      schema = arrowSchemaToStructType(vectorSchemaRootReceived.getSchema)
      val rowCount = vectorSchemaRootReceived.getRowCount
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala

      Seq.range(0, rowCount).foreach { rowIndex =>
        val rowValues = fieldVectors.map { vector =>
          if (vector.isNull(rowIndex)) {
            null
          } else {
            vector match {
              case v: VarCharVector =>
                val strValue = v.getObject(rowIndex).toString
                if (v.getField.getMetadata.isEmpty) strValue else DFRef(strValue)
              case v: IntVector => v.get(rowIndex)
              case v: BigIntVector => v.get(rowIndex)
              case v: Float4Vector => v.get(rowIndex)
              case v: Float8Vector => v.get(rowIndex)
              case v: BitVector => v.get(rowIndex) != 0
              case v: VarBinaryVector => v.get(rowIndex)
              case _ => throw new UnsupportedOperationException(
                s"Unsupported type: ${vector.getClass}"
              )
            }
          }
        }
        allRows += Row.fromSeq(rowValues)
      }
    }
    DefaultDataFrame(schema, ClosableIterator(allRows.toIterator)())
  }
}
