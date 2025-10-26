package link.rdcn.server

import link.rdcn.struct.{Column, DataFrame, DefaultDataFrame, Row, StructType, ValueType}
import link.rdcn.struct.ValueType.{BinaryType, BlobType, BooleanType, DoubleType, FloatType, IntType, LongType, RefType, StringType}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.inferValueType
import org.apache.arrow.flight.{FlightProducer, FlightStream, Result}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.{FloatingPointPrecision, Types}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.json.JSONObject

import java.io.ByteArrayOutputStream
import java.util.Collections
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter, asScalaIteratorConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 10:56
 * @Modified By:
 */
object ServerUtils {

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

  //accept put data
  def flightStreamToRowIterator(flightStream: FlightStream): Iterator[Row] = new Iterator[Row] {
    private var currentRoot: VectorSchemaRoot = _
    private var rowIndex = 0
    private var totalRowsInRoot = 0

    private def loadNextBatch(): Boolean = {
      if (flightStream.next()) {
        currentRoot = flightStream.getRoot
        rowIndex = 0
        totalRowsInRoot = currentRoot.getRowCount
        true
      } else false
    }

    override def hasNext: Boolean = {
      (currentRoot != null && rowIndex < totalRowsInRoot) || loadNextBatch()
    }

    override def next(): Row = {
      if (!hasNext) throw new NoSuchElementException
      val row = Row(
        currentRoot.getFieldVectors.asScala.map { vector =>
          vector.getObject(rowIndex) match {
            case t: org.apache.arrow.vector.util.Text => t.toString
            case other => other
          }
        }
      )
      rowIndex += 1
      row
    }
  }

  //convert action result to dataFrame
  def sendDataFrame(df: DataFrame, listener: FlightProducer.StreamListener[Result], allocator: BufferAllocator): Unit = {
    val structType = df.schema
    val schema = convertStructTypeToArrowSchema(structType)
    val childAllocator: BufferAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    try {
      root.allocateNew() // 分配内存

      var rowIndex = 0
      df.mapIterator(stream => {
        stream.foreach { row =>
          structType.columns.zipWithIndex.foreach { case (col, colIndex) =>
            val vector = root.getVector(col.name)
            col.colType match {
              case ValueType.StringType =>
                vector.asInstanceOf[VarCharVector].setSafe(rowIndex, row.get(colIndex).toString.getBytes("UTF-8"))
              case ValueType.IntType =>
                vector.asInstanceOf[IntVector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Int])
              case ValueType.LongType =>
                vector.asInstanceOf[BigIntVector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Long])
              case ValueType.FloatType =>
                vector.asInstanceOf[Float4Vector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Float])
              case ValueType.DoubleType =>
                vector.asInstanceOf[Float8Vector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Double])
              case ValueType.BooleanType =>
                vector.asInstanceOf[BitVector].setSafe(rowIndex, if (row.get(colIndex).asInstanceOf[Boolean]) 1 else 0)
              case ValueType.BinaryType =>
                vector.asInstanceOf[VarBinaryVector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Array[Byte]])
              case _ =>
                throw new UnsupportedOperationException(s"Unsupported type: ${col.colType}")
            }
          }
          rowIndex += 1
        }
      })
      root.setRowCount(rowIndex)

      listener.onNext(new Result(ServerUtils.getBytesFromVectorSchemaRoot(root)))
      listener.onCompleted()
    } finally {
      root.close()
      allocator.close()
    }
  }

  def getBytesFromVectorSchemaRoot(root: VectorSchemaRoot): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, outputStream)
    writer.start()
    writer.writeBatch()
    writer.end()
    writer.close()
    outputStream.toByteArray
  }
}
