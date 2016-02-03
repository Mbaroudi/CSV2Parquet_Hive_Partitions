package CSV2Parquet

/**
 * Created by jmlopez on 22/01/16.
 */

/**
 * Let's say we have this a HDFS - HIVE table partitioned like this. All the data is stored like text (CSV):
 * root
 *  |--year
 *  |--month
 *  |--day
 *  |--hour
 *
 * And we want to create:
 * 1) A parquet file containing all the data
 * 2) Exactly the same partitioning but with Parquet files instead text (CSV) files.
 *
 * We'll construct a Spark Job for this two tasks.
 */

import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Csv2Parquet extends App {

  /**
   * This is the root path
   *  * rootPath
   *  |--year
   *  |--month
   *  |--day
   *  |--hour/ *.txt <--- Text Files
   *
   *  This will be the result:
   *  * dstPath
   *  |--year
   *  |--month
   *  |--day
   *  |--hour/ *.parquet
   *
   * @param sc SparkContext
   * @param rootPath Folder from where the job will start to search files
   * @param dstPath Folder where the job will put the table result
   */
  def CSV2ParquetAll(sc: SparkContext, rootPath: String, dstPath: String): Unit = {
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.
      option("mergeSchema", "true").
      option("spark.sql.sources.partitionColumnTypeInference.enabled", "true").
      load(rootPath)

    // For writting the data, we merge schema to have schema evolution
    // http://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging
    df.write.option("mergeSchema", "true").parquet(dstPath)
  }


  /**
   * This is the root path
   *  * rootPath
   *  |--year
   *  |--month
   *  |--day
   *  |--hour/ *.txt <--- Text Files
   *
   *  This will be the result:
   *  * dstPath
   *  |--year
   *  |--month
   *  |--day
   *  |--hour/ *.parquet
   *
   * @param sc SparkContext
   * @param rootPath Folder from where the job will start to search files
   * @param dstPath Folder where the job will put the table result with the same partitions
   */
  def CSV2Parquet(sc: SparkContext, rootPath: String, dstPath: String): Unit ={
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)


    val years = Seq("2015", "2016")
    val months = (1 to 12).toList.reverse
    val days = (1 to 31).toList
    val hours = (0 to 23).toList

    val paths: Seq[String] =
      for {
        year <- years
        month <- months
        day <- days
        hour <- hours
      } yield rootPath + s"year=$year/month=$month/day=$day/hour=$hour/"

    case class event(eventId: String,
                     timestamp: String,
                     eventType: String)

    val event_schema =
      StructType(
          StructField("eventId", StringType, true) ::
          StructField("timestamp", StringType, true) ::
          StructField("eventType", StringType, true) ::
          Nil)

    for (pth <- paths){
      val fs2 = FileSystem.get(new java.net.URI(pth), sc.hadoopConfiguration)
      val pth_hadoop = new org.apache.hadoop.fs.Path(pth)

      val dst_hadoop = new org.apache.hadoop.fs.Path(dstPath)
      if (fs2.exists(pth_hadoop) && !fs2.exists(dst_hadoop)) {
        val status = fs2.listStatus(pth_hadoop)
        if (status.length > 0) {
          val df = sqlContext.read.
            option("mergeSchema", "true").
            option("spark.sql.sources.partitionColumnTypeInference.enabled", "true").
            load(pth)

          // For writting the data, we merge schema to have schema evolution
          // http://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging
          df.write.option("mergeSchema", "true").parquet(dstPath)
        } else {
          println("This directory is empty: " + pth)
        }
      }
    }
  }
}

object Text2Parquet {
  def main(args: Array[String]) {
    if (args.length != 2 ) {
      System.err.println (
        "Usage: Text2Parquet <rootPath> <rootDestinationPath>")
      System.err.println (
        " Or Usage: Text2Parquet <rootPath> <rootDestinationPath>")
      System.exit (1)
    }

    val Seq (host, port) = args.toSeq
    val sparkConf = new SparkConf ().setAppName ("Text2Parquet")
    val sc: SparkContext = new SparkContext(sparkConf)

    Csv2Parquet.CSV2Parquet(sc, "https://10.0.0.1/rootPath", "https://10.0.0.1/dstPath" )

  }
}
