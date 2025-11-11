package link.rdcn.struct

import link.rdcn.util.{DataUtils, JdbcUtils}

import java.io.{BufferedReader, File, FileReader}
import java.nio.file.attribute.BasicFileAttributes
import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}
import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */

trait DataStreamSource {
  final def dataFrame: DataFrame = DefaultDataFrame(schema, iterator)

  def rowCount: Long

  def schema: StructType

  def iterator: ClosableIterator[Row]
}

object DataStreamSource {

  private val sampleSize = 10
  private val jdbcFetchSize = 500

  def guessCsvDelimiter(sampleLines: Seq[String]): String = {
    val candidates = List(",", ";", "\t", "\\|")
    candidates.maxBy{
      delim => sampleLines.head.split(delim).length
    }
  }

  def csv(csvFile: File, delimiter: Option[String] = None, header: Boolean = true): DataStreamSource = {
    val fileRowCount = DataUtils.countLinesFast(csvFile)
    val iterLines: ClosableIterator[String] = DataUtils.getFileLines(csvFile)
    var sampleLines: Seq[String] = iterLines.take(10).toSeq
    val delim = delimiter.getOrElse(guessCsvDelimiter(sampleLines))
    val headerArray = new ArrayBuffer[String]()
    if (header) {
      sampleLines.head.split(delim, -1).map(headerArray.append(_))
      sampleLines = sampleLines.tail
    }

    val sampleBuffer = sampleLines.map(_.split(delim, -1)).toArray
    val structType = DataUtils.inferSchema(sampleBuffer, headerArray)

    val sampleRows = sampleBuffer.iterator.map(arr => Row.fromSeq(arr.toSeq))
    val remainingRows = iterLines.map(_.split(delim, -1)).map(arr => Row.fromSeq(arr.toSeq))

    val iterRows = (sampleRows ++ remainingRows).map(DataUtils.convertStringRowToTypedRow(_, structType))
    new DataStreamSource {
      override def rowCount: Long = fileRowCount

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows)(iterLines.onClose)
    }
  }

  def excel(excelPath: String): DataStreamSource = {
    val structType = DataUtils.inferExcelSchema(excelPath)
    val iterRows = DataUtils.readExcelRows(excelPath, structType)
    new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows.map(Row.fromSeq(_)))()
    }
  }

  def filePath(dir: File, recursive: Boolean = false): DataStreamSource = {
    var iterFiles: Iterator[(File, BasicFileAttributes)] = Iterator.empty
    iterFiles = if (recursive) DataUtils.listAllFilesWithAttrs(dir)
    else DataUtils.listFilesWithAttributes(dir).toIterator
    val stream = iterFiles
      // schema [name, byteSize, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
      .map { file =>
        (file._1.getName, file._2.size(),
          DataUtils.getFileType(file._1), file._2.creationTime().toMillis,
          file._2.lastModifiedTime().toMillis, file._2.lastAccessTime().toMillis,
          Blob.fromFile(file._1))
      }
      .map(Row.fromTuple(_))
    new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = StructType.binaryStructType

      override def iterator: ClosableIterator[Row] =
        new ClosableIterator(stream, () => iterFiles.map(file => file._1.delete()), false)
    }
  }

  def sqlTable(
                jdbcUrl: String,
                user: String,
                password: String,
                tableName: String,
                driver: String = "com.mysql.cj.jdbc.Driver"
              ): Unit = {
    Class.forName(driver)
    val conn: Connection = DriverManager.getConnection(jdbcUrl, user, password)
    val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // 可配置 fetchSize 优化大数据查询
    stmt.setFetchSize(jdbcFetchSize)

    val rs = stmt.executeQuery(s"SELECT * FROM $tableName")
    val rsMeta = rs.getMetaData
    val structType = JdbcUtils.inferSchema(rsMeta)
    val iterRows: Iterator[Row] = JdbcUtils.resultSetToIterator(rs, stmt, conn, structType)

    new DataStreamSource {
      override def rowCount: Long = -1 // 行数未知，除非 COUNT 查询

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows)(() => {
        rs.close()
        stmt.close()
        conn.close()
      })
    }
  }

}


