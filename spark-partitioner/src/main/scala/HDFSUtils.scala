import java.io.{BufferedReader, IOException, InputStreamReader}

import com.google.common.io.ByteStreams
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ylu on 9/12/16.
  */
object HDFSUtils {

  def hadoopHome = Global.HadoopHome

  def readFile(filename: String): ArrayBuffer[String] = {
    val hdfs = getFS()
    val br: BufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(filename))))
    var lines = ArrayBuffer[String]()
    try {
      var line: String = null
      line = br.readLine
      while (line != null) {
        lines += line
        line = br.readLine
      }
      lines
    }
    catch {
      case e => {
        e.printStackTrace()
        throw new RuntimeException("Could not read from file:" + filename)
      }
    }
    finally {
      br.close()
    }
  }

  def readFileToBytes(filename: String): Array[Byte] = {
    val hdfs = getFS()
    try {
      val in: FSDataInputStream = hdfs.open(new Path(filename))
      val bytes: Array[Byte] = ByteStreams.toByteArray(in)
      in.close()
      bytes
    }
    catch {
      case e => {
        e.printStackTrace()
        throw new RuntimeException("Could not read from file:" + filename)
      }
    }
  }


  def rename(dest: String): Unit = {
    val hdfs = getFS()
    try {
      // delete _SUCCESS

      hdfs.delete(new Path(dest + "/_SUCCESS"), false);
      val fileStatus = hdfs.listStatus(new Path(dest));

      fileStatus.foreach(
        x => {
          val oldPath = x.getPath().toString();
          val baseName = FilenameUtils.getBaseName(oldPath);
          val dir = oldPath.substring(0, oldPath.length() - baseName.length());
          val newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));
          hdfs.rename(new Path(oldPath), new Path(newPath));
        }
      )
    } catch {
      case e => {
        e.printStackTrace()
      }
    }

  }

  private def getFS(): FileSystem = {
    try {
      val conf: Configuration = new Configuration
      val coreSitePath = hadoopHome + "/etc/hadoop/core-site.xml"
      conf.addResource(new Path(coreSitePath))
      FileSystem.get(conf)
    }
    catch {
      case e: Exception => {
        throw new RuntimeException("Failed to get the HDFS Filesystem! " + e.getMessage)
      }
    }
  }
}