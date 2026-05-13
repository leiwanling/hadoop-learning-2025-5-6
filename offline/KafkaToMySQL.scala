//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.streaming.Trigger
//
//object KafkaToMySQL {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("KafkaToMySQL")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // 定义 JSON 的 schema（与 Kafka 消息字段一致）
//    val schema = new StructType()
//      .add("user_id", StringType)
//      .add("item_id", StringType)
//      .add("category_id", StringType)
//      .add("behavior_type", StringType)
//      .add("quantity", IntegerType)
//      .add("amount", DoubleType)
//      .add("timestamp", LongType)
//      .add("session_id", StringType)
//
//    // 从 Kafka 读取流数据，并解析 JSON
//    val rawDF = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.32.100:9092")
//      .option("subscribe", "user_behavior")
//      .option("startingOffsets", "latest")
//      .load()
//      .select(from_json($"value".cast("string"), schema).alias("data"))
//      .select("data.*")
//      .withColumn("event_time", from_unixtime($"timestamp" / 1000).cast("timestamp"))
//
//    // 添加水印（30分钟延迟，适配历史旧数据），进行5分钟滚动窗口（每1分钟滑动一次）聚合
//    val windowed = rawDF
//      .withWatermark("event_time", "30 minutes")
//      .groupBy(window($"event_time", "5 minutes", "1 minute"))
//      .agg(
//        sum(when($"behavior_type" === "click", 1).otherwise(0)).alias("pv"),
//        approx_count_distinct("user_id").alias("uv"),
//        sum(when($"behavior_type" === "buy", $"amount").otherwise(0)).alias("sales")
//      )
//      .select($"window.start", $"window.end", $"pv", $"uv", $"sales")
//
//    // 自定义写入 MySQL 的函数（使用 REPLACE INTO 避免主键重复）
//    def writeToMySQL(df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long): Unit = {
//      println(s"=== Processing batch $batchId with ${df.count()} rows ===")
//      if (df.count() > 0) {
//        df.show(10, false)
//
//        // 使用 foreachPartition 批量写入，提高性能
//        df.foreachPartition { partition =>
//          if (partition.nonEmpty) {
//            val url = "jdbc:mysql://192.168.32.100:3306/realtime_db?useSSL=false"
//            val user = "root"
//            val password = "123456"
//            var connection: java.sql.Connection = null
//            var stmt: java.sql.PreparedStatement = null
//            try {
//              connection = java.sql.DriverManager.getConnection(url, user, password)
//              // 注意：要求 realtime_stats 表有唯一索引或主键 (start, end)
//              stmt = connection.prepareStatement(
//                """REPLACE INTO realtime_stats (start, end, pv, uv, sales)
//                  |VALUES (?, ?, ?, ?, ?)""".stripMargin)
//              partition.foreach { row =>
//                stmt.setTimestamp(1, row.getAs[java.sql.Timestamp]("start"))
//                stmt.setTimestamp(2, row.getAs[java.sql.Timestamp]("end"))
//                stmt.setLong(3, row.getAs[Long]("pv"))
//                stmt.setLong(4, row.getAs[Long]("uv"))
//                stmt.setDouble(5, row.getAs[Double]("sales"))
//                stmt.executeUpdate()
//              }
//            } finally {
//              if (stmt != null) stmt.close()
//              if (connection != null) connection.close()
//            }
//          }
//        }
//        println(s"Batch $batchId has been written to MySQL successfully.")
//      } else {
//        println(s"Batch $batchId is empty, nothing written.")
//      }
//    }
//
//    // 启动流查询：使用 complete 模式（每个批次输出所有窗口），因为历史数据延迟较大，complete 模式能立即输出
//    val query = windowed.writeStream
//      .foreachBatch(writeToMySQL _)
//      .outputMode("complete")
//      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .start()
//
//    query.awaitTermination()
//  }
//}




import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, sum, countDistinct, desc}
import java.util.Properties
import java.sql.{Connection, DriverManager, PreparedStatement}

object DailyOfflineAnalysis {
  def main(args: Array[String]): Unit = {
    // 1. 解析日期参数（默认昨天）
    val date = if (args.length > 0) args(0) else {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      format.format(new java.util.Date(System.currentTimeMillis() - 86400000))
    }
    println(s"Processing offline analysis for date: $date")

    // 2. 创建 SparkSession（使用默认文件系统，由 spark-submit 的 --conf spark.hadoop.fs.defaultFS 指定）
    val spark = SparkSession.builder()
      .appName(s"DailyOfflineAnalysis-$date")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    // 3. 读取 HDFS 上的原始 JSON（使用相对路径，依赖默认文件系统）
    //    路径格式：/data/original/behavior/dt=2026-05-13/hour=00/events*.log
    val rawPath = s"/data/original/behavior/dt=$date/*/*.log"
    val rawDF = spark.read.schema(getSchema).json(rawPath)

    // 添加 dt 分区列
    val rawDFWithDt = rawDF.withColumn("dt", lit(date))

    // 将原始数据转换为 Parquet 并存储（按 dt 分区）
    val parquetBasePath = "/data/warehouse/behavior_parquet"
    rawDFWithDt.write
      .mode("overwrite")
      .partitionBy("dt")
      .parquet(parquetBasePath)

    // 后续分析直接读取当天的 Parquet 数据（指向 dt 分区目录）
    val df = spark.read.parquet(s"$parquetBasePath/dt=$date/*")

    // 4. 缓存数据（多个聚合操作复用）
    df.cache()

    // 5. 各项统计
    // 5.1 每日总销售额
    val totalSales = df.filter($"behavior_type" === "buy")
      .agg(sum($"amount").alias("total_sales"))
      .collect()(0).getDouble(0)

    // 5.2 热门商品 Top 10（按购买金额）
    val topProducts = df.filter($"behavior_type" === "buy")
      .groupBy($"item_id")
      .agg(sum($"amount").alias("sales"))
      .orderBy($"sales".desc)
      .limit(10)
      .collect()
      .map(row => (row.getString(0), row.getDouble(1)))

    // 5.3 用户行为漏斗（各行为类型的独立用户数）
    val funnel = df.groupBy($"behavior_type")
      .agg(countDistinct($"user_id").alias("uv"))
      .collect()
      .map(row => (row.getString(0), row.getLong(1)))
      .toMap

    // 5.4 各品类销售额（可选，本代码未使用但保留）
    val categorySales = df.filter($"behavior_type" === "buy")
      .groupBy($"category_id")
      .agg(sum($"amount").alias("sales"))
      .orderBy($"sales".desc)
      .collect()
      .map(row => (row.getString(0), row.getDouble(1)))

    // 6. 写入 MySQL 日报表
    val mysqlUrl = "jdbc:mysql://192.168.32.100:3306/realtime_db?useSSL=false"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 准备 Top3 数据
    val top1 = if (topProducts.length > 0) (topProducts(0)._1, topProducts(0)._2) else ("", 0.0)
    val top2 = if (topProducts.length > 1) (topProducts(1)._1, topProducts(1)._2) else ("", 0.0)
    val top3 = if (topProducts.length > 2) (topProducts(2)._1, topProducts(2)._2) else ("", 0.0)

    // 使用 REPLACE INTO 写入 MySQL（表必须已存在，建议事先创建）
    var conn: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(mysqlUrl, props)
      val replaceSQL = """
        REPLACE INTO daily_reports
        (report_date, total_sales, top1_product, top1_sales, top2_product, top2_sales,
         top3_product, top3_sales, click_uv, cart_uv, buy_uv)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
      val pstmt = conn.prepareStatement(replaceSQL)
      pstmt.setString(1, date)
      pstmt.setDouble(2, totalSales)
      pstmt.setString(3, top1._1); pstmt.setDouble(4, top1._2)
      pstmt.setString(5, top2._1); pstmt.setDouble(6, top2._2)
      pstmt.setString(7, top3._1); pstmt.setDouble(8, top3._2)
      pstmt.setLong(9, funnel.getOrElse("click", 0L))
      pstmt.setLong(10, funnel.getOrElse("cart", 0L))
      pstmt.setLong(11, funnel.getOrElse("buy", 0L))
      pstmt.executeUpdate()
      println(s"Report for $date inserted successfully")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) conn.close()
    }

    // 7. 释放缓存并关闭 Spark
    df.unpersist()
    spark.stop()
  }

  // JSON 的 schema 定义
  def getSchema: org.apache.spark.sql.types.StructType = {
    import org.apache.spark.sql.types._
    new StructType()
      .add("user_id", StringType)
      .add("item_id", StringType)
      .add("category_id", StringType)
      .add("behavior_type", StringType)
      .add("quantity", IntegerType)
      .add("amount", DoubleType)
      .add("timestamp", LongType)
      .add("session_id", StringType)
  }
}