package com.jd.easy.audience.task.plugin.step.readdata

import java.nio.charset.StandardCharsets.UTF_8

import net.jpountz.lz4.LZ4BlockInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.net.URI

import org.apache.hadoop.fs.FileSystem

import scala.util.control.Breaks.break
object EventLogParse {

  final val BLOCK_SIZE = 23 * 1024

  val buf = new Array[Byte](BLOCK_SIZE)

  def main(av: Array[String]): Unit = {
    println("application path" + av(0))
    val hdfsPath = av(0)


    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(URI.create(hdfsPath), conf)
    val in = fs.open(new Path(hdfsPath))
    val bis = new LZ4BlockInputStream(in)

    try {
      Iterator.continually(bis.read(buf))
        .takeWhile(_ != 0)
        .foreach { len =>
          if (len > 0) {
            val str = new String(buf, 0, len, UTF_8)
            print(str)
          } else {
            break;
          }
        }
    }
    finally {
      bis.close()
    }
  }

}
