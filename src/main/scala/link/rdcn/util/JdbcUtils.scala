package link.rdcn.util

import link.rdcn.struct.ValueType._
import link.rdcn.struct._

import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/8 16:22
 * @Modified By:
 */
object JdbcUtils {

  def inferSchema(rsMeta: ResultSetMetaData): StructType = {
    val columns = for (i <- 1 to rsMeta.getColumnCount) yield {
      val name = rsMeta.getColumnName(i)
      val sqlType = rsMeta.getColumnType(i)
      val nullable = rsMeta.isNullable(i) != ResultSetMetaData.columnNoNulls

      val valueType = sqlType match {
        case java.sql.Types.INTEGER => IntType
        case java.sql.Types.BIGINT => LongType
        case java.sql.Types.DOUBLE |
             java.sql.Types.FLOAT => DoubleType
        case java.sql.Types.BOOLEAN => BooleanType
        case java.sql.Types.DATE |
             java.sql.Types.TIMESTAMP |
             java.sql.Types.VARCHAR |
             java.sql.Types.CHAR |
             java.sql.Types.NVARCHAR |
             java.sql.Types.LONGVARCHAR => StringType
        case _ => StringType
      }

      Column(name, valueType, nullable)
    }
    StructType.fromSeq(columns)
  }

  def resultSetToIterator(rs: ResultSet, stmt: Statement, conn: Connection, schema: StructType): Iterator[Row] = new Iterator[Row] {
    private var finished = false

    override def hasNext: Boolean = {
      val hn = rs.next()
      if (!hn && !finished) {
        finished = true
        // 自动释放资源
        try rs.close() catch {
          case _: Throwable =>
        }
        try stmt.close() catch {
          case _: Throwable =>
        }
        try conn.close() catch {
          case _: Throwable =>
        }
      }
      hn
    }

    override def next(): Row = {
      val values = schema.columns.zipWithIndex.map { case (col, idx) =>
        val i = idx + 1
        if (rs.getObject(i) == null) null
        else col.colType match {
          case IntType => rs.getInt(i)
          case LongType => rs.getLong(i)
          case DoubleType => rs.getDouble(i)
          case BooleanType => rs.getBoolean(i)
          case _ => rs.getString(i)
        }
      }
      Row.fromSeq(values)
    }
  }

}
