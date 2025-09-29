/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:08
 * @Modified By:
 */
package link.rdcn

import link.rdcn.ServerTestBase.{binFileCount, csvFileCount, getOutputDir}

import java.io.{BufferedWriter, FileOutputStream, FileWriter, IOException}
import java.nio.file.{Files, Paths}
import java.util.Comparator


object ServerTestDataGenerator {
  def generateTestData(binDir: String, csvDir: String, baseDir: String): Unit = {
    println("Starting test data generation...")
    val startTime = System.currentTimeMillis()

    createDirectories(binDir, csvDir, baseDir)
    generateBinaryFiles(binDir)
    generateCsvFiles(csvDir)

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Test data generation completed in ${duration}s")
    printDirectoryInfo(binDir, csvDir)
  }

  def createDirectories(binDir: String, csvDir: String, baseDir: String): Unit = {
    Files.createDirectories(Paths.get(binDir))
    Files.createDirectories(Paths.get(csvDir))
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
  }
}
