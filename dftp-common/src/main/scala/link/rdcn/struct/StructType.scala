package link.rdcn.struct

import link.rdcn.struct.ValueType._

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 14:55
 * @Modified By:
 */
case class Column(name: String, colType: ValueType, nullable: Boolean = true)

case class StructType(val columns: Seq[Column]) {

  private val nameToIndex: Map[String, Int] = {
    val pairs = columns.zipWithIndex.map { case (col, idx) => col.name -> idx }
    val dupNames = pairs.groupBy(_._1).collect { case (n, xs) if xs.size > 1 => n }
    require(dupNames.isEmpty, s"StructType 构造失败：存在重复列名 ${dupNames.mkString(",")}")
    pairs.toMap
  }

  def isEmpty(): Boolean = columns.isEmpty

  /** 根据列名获取类型（可选） */
  def getType(columnName: String): Option[ValueType] =
    columns.find(_.name == columnName).map(_.colType)

  /** 判断某列是否存在 */
  def contains(columnName: String): Boolean =
    columns.exists(_.name == columnName)

  def contains(colType: ValueType): Boolean =
    columns.exists(_.colType == colType)

  /** 返回所有列名 */
  def columnNames: Seq[String] = columns.map(_.name)

  /** 根据列名获取索引，第几列，从 0 开始；找不到返回 None */
  def indexOf(columnName: String): Option[Int] =
    nameToIndex.get(columnName)

  /** 根据索引获取列定义；越界时抛异常 */
  def columnAt(index: Int): Column =
    if (index >= 0 && index < columns.length) columns(index)
    else throw new IndexOutOfBoundsException(s"columnAt: 索引 $index 越界，列数 ${columns.length}")

  def add(name: String, colType: ValueType, nullable: Boolean = true): StructType =
    this.copy(columns = columns :+ Column(name, colType, nullable))

  def select(columnNames: String*): StructType = {
    val selected = columnNames.map { name =>
      nameToIndex.get(name) match {
        case Some(idx) => columns(idx)
        case None => throw new IllegalArgumentException(s"StructType.select: 列名 '$name' 不存在")
      }
    }
    StructType.fromSeq(selected)
  }

  def prepend(column: Column): StructType = new StructType(column +: columns)

  def append(column: Column): StructType = new StructType(columns :+ column)

  override def toString: String =
    columns.map(c => s"${c.name}: ${c.colType}").mkString("schema(", ", ", ")")
}

object StructType {
  /** varargs 构造：StructType(Column("id", IntType), ...) */
  def fromColumns(cols: Column*): StructType = new StructType(cols)

  /** 避免与 case class 自动生成 apply 冲突 */
  def fromSeq(cols: Seq[Column]): StructType = new StructType(cols)

  def fromNamesAndTypes(pairs: (String, ValueType)*): StructType =
    new StructType(pairs.map { case (n, t) => Column(n, t) })

  def fromNamesAsAny(names: Seq[String]): StructType =
    new StructType(names.map(n => Column(n, ValueType.StringType)))

  /**
   * 从字符串解析 StructType。
   * 期望的格式: "schema(col1: Type1, col2: Type2, ...)"
   * 例如: "schema(id: IntType, name: StringType)"
   */
  def fromString(schemaString: String): StructType = {
    if (schemaString == "schema()") {
      return StructType.empty
    }

    // 检查并移除前缀和后缀
    val prefix = "schema("
    val suffix = ")"
    if (!schemaString.startsWith(prefix) || !schemaString.endsWith(suffix)) {
      throw new IllegalArgumentException(s"无效的 StructType 字符串格式: '$schemaString'。期望格式: 'schema(col1: Type1, ...)'")
    }

    val content = schemaString.substring(prefix.length, schemaString.length - suffix.length).trim
    val columnPattern = "(.+):\\s*([a-zA-Z]+)".r // 匹配列名和类型，例如 "id: IntType"

    if (content.isEmpty) { // 处理 "schema()" 的情况
      StructType.empty
    } else {
      // 按逗号和空格 ", " 分割每个列定义
      val columnDefs = content.split(",\\s*").map(_.trim)

      val parsedColumns = columnDefs.map { colDef =>
        colDef match {
          case columnPattern(name, typeName) =>
            val colType = ValueTypeHelper.fromName(typeName) // 使用 ValueType 的 fromString 方法
            Column(name, colType) // 默认 nullable 为 true
          case _ =>
            throw new IllegalArgumentException(s"无效的列定义格式: '$colDef'。期望格式: 'name: Type'")
        }
      }
      StructType.fromSeq(parsedColumns.toSeq)
    }
  }

  val empty: StructType = new StructType(Seq.empty)

  def blobStreamStructType: StructType = StructType.empty.add("content", BinaryType)

  def binaryStructType: StructType = {
    StructType.empty.add("name", StringType).add("byteSize", LongType).add("type", StringType)
      .add("creationTime", LongType).add("lastModifiedTime", LongType).add("lastAccessTime", LongType)
      .add("File", BlobType)
  }
}
