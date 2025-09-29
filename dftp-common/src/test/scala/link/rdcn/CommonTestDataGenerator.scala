/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:08
 * @Modified By:
 */
package link.rdcn



import link.rdcn.CommonTestBase._
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import java.io.{BufferedWriter, FileOutputStream, FileWriter, IOException}
import java.nio.file.{Files, Paths}
import java.util.{Comparator, Random}


object CommonTestDataGenerator {
  def generateTestData(binDir: String, csvDir: String, excelDir: String, baseDir: String, totalLines: Int): Unit = {
    println("Starting test data generation...")
    val startTime = System.currentTimeMillis()

    createDirectories(binDir, csvDir, excelDir, baseDir)
    generateBinaryFiles(binDir)
    generateCsvFiles(csvDir)
    generateExcelFiles(excelDir, totalLines)

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Test data generation completed in ${duration}s")
    printDirectoryInfo(binDir, csvDir)
  }

  def createDirectories(binDir: String, csvDir: String, excelDir: String, baseDir: String): Unit = {
    Files.createDirectories(Paths.get(binDir))
    Files.createDirectories(Paths.get(csvDir))
    Files.createDirectories(Paths.get(excelDir))
    println(s"Created directory structure at ${Paths.get(baseDir).toAbsolutePath}")
  }

  def generateBinaryFiles(binDir: String): Unit = {
    println(s"Generating $binFileCount binary files (~1GB each)...")
    (1 to binFileCount).foreach { i =>
      val fileName = s"binary_data_$i.bin"
      val filePath = Paths.get(binDir).resolve(fileName)
      val startTime = System.currentTimeMillis()
      val size = 1024 * 1024 * 1024 // 1GB
      var fos: FileOutputStream = null
      try {
        fos = new FileOutputStream(filePath.toFile)
        val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
        var bytesWritten = 0L
        while (bytesWritten < size) {
          fos.write(buffer)
          bytesWritten += buffer.length
        }
      } finally {
        if (fos != null) fos.close()
      }
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"   Generated ${filePath.getFileName} (${formatSize(size)}) in ${duration}s")
    }
  }


  def generateCsvFiles(csvDir: String): Unit = {
    println(s"Generating $csvFileCount CSV files with 100 million rows each...")
    (1 to csvFileCount).foreach { i =>
      val fileName = s"data_$i.csv"
      val filePath = Paths.get(csvDir).resolve(fileName).toFile
      val startTime = System.currentTimeMillis()
      val rows = 10000 // 1 亿行
      var writer: BufferedWriter = null // 声明为 var，方便 finally 块中访问

      try {
        writer = new BufferedWriter(new FileWriter(filePath), 1024 * 1024) // 1MB 缓冲区
        writer.write("id,value\n") // 写入表头

        for (row <- 1 to rows) {
          writer.append(row.toString).append(',').append(math.random.toString).append('\n')
          if (row % 1000000 == 0) writer.flush() // 每百万行刷一次
        }

        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        println(f"   Generated ${filePath.getName} with $rows rows in $duration%.2fs")

      } catch {
        case e: Exception =>
          println(s"Error generating file ${filePath.getName}: ${e.getMessage}")
          throw e

      } finally {
        if (writer != null) {
          try writer.close()
          catch {
            case e: Exception => println(s"Error closing writer: ${e.getMessage}")
          }
        }
      }
    }
  }

  def generateExcelFiles(excelDir: String, totalRows: Int = 100): Unit = {
    println(s"Generating $excelFileCount Excel file(s) with $totalRows rows each...")

    (1 to excelFileCount).foreach { i =>
      val fileName = s"data_$i.xlsx"
      val filePath = Paths.get(excelDir).resolve(fileName).toFile
      val startTime = System.currentTimeMillis()

      // XSSFWorkbook 用于处理 .xlsx 格式
      val workbook = new XSSFWorkbook()
      val sheet = workbook.createSheet("DataSheet")
      var fileOutputStream: FileOutputStream = null

      try {
        // 1. 写入表头
        val headerRow = sheet.createRow(0)
        headerRow.createCell(0).setCellValue("id")
        headerRow.createCell(1).setCellValue("value")

        // 2. 写入数据行
        // 注意：Excel 库会自动处理内部缓冲区，但大数据量时 POI 性能会受到影响
        for (rowNum <- 1 to totalRows) {
          val dataRow = sheet.createRow(rowNum)

          // 写入 ID (整数类型)
          dataRow.createCell(0).setCellValue(rowNum.toDouble)

          // 写入 Value (浮点数)
          dataRow.createCell(1).setCellValue(math.random)

          // 模拟 CSV 中的进度刷新（仅为打印进度，对于 POI XSSFWorkbook 实际作用有限）
          if (rowNum % 100000 == 0) {
            print(s"\r   Writing ${filePath.getName}: ${rowNum} rows...")
          }
        }

        // 3. 将工作簿写入文件
        fileOutputStream = new FileOutputStream(filePath)
        workbook.write(fileOutputStream)

        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        println(f"\r   Generated ${filePath.getName} with $totalRows rows in $duration%.2fs")

      } catch {
        case e: Exception =>
          println(s"Error generating file ${filePath.getName}: ${e.getMessage}")
          throw e

      } finally {
        // 确保所有资源关闭
        if (workbook != null) {
          try workbook.close() // 关闭工作簿
          catch { case e: Exception => println(s"Error closing workbook: ${e.getMessage}") }
        }
        if (fileOutputStream != null) {
          try fileOutputStream.close() // 关闭文件输出流
          catch { case e: Exception => println(s"Error closing file stream: ${e.getMessage}") }
        }
      }
    }
  }


  def formatSize(bytes: Long): String = {
    if (bytes < 1024) s"${bytes}B"
    else if (bytes < 1024 * 1024) s"${bytes / 1024}KB"
    else if (bytes < 1024 * 1024 * 1024) s"${bytes / (1024 * 1024)}MB"
    else s"${bytes / (1024 * 1024 * 1024)}GB"
  }

  def printDirectoryInfo(binDir: String, csvDir: String): Unit = {
    println("\n Generated Data Summary:")
    printDirectorySize(binDir, "Binary Files")
    printDirectorySize(csvDir, "CSV Files")
    println("----------------------------------------\n")
  }

  def printDirectorySize(dirString: String, label: String): Unit = {
    val dir = Paths.get(dirString)
    if (Files.exists(dir)) {
      val size = Files.walk(dir)
        .filter(p => Files.isRegularFile(p))
        .mapToLong(p => Files.size(p))
        .sum()
      println(s"   $label: ${formatSize(size)} (${Files.list(dir).count()} files)")
    }
  }

  // 清理所有测试数据
  def cleanupTestData(baseDir: String): Unit = {
    println("Cleaning up test data...")
    val startTime = System.currentTimeMillis()
    val basePath = Paths.get(baseDir)

    if (Files.exists(Paths.get(getOutputDir("test_output")))) {
      try {
        Files.walk(basePath)
          .sorted(Comparator.reverseOrder())
          .forEach(path => {
            try {
              Files.delete(path)
            } catch {
              case e: IOException =>
                // 捕获并处理删除文件时的错误
                println(s"Error deleting file/dir: $path. Error: ${e.getMessage}")
                throw e // 可以选择重新抛出或记录日志
            }
          })
        println(s"Successfully deleted directory: $basePath")
      } catch {
        case e: IOException =>
          // 处理遍历或顶层目录删除失败的错误
          println(s"Failed to process directory $basePath: ${e.getMessage}")
          throw e
      }
    }

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Cleanup completed in ${duration}s")
  }

}
