import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object sparksteaming_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToMySQL")
      .getOrCreate()

    import spark.implicits._

    // 定义 JSON 的 schema（与 Kafka 消息字段一致）
    val schema = new StructType()
      .add("user_id", StringType)
      .add("item_id", StringType)
      .add("category_id", StringType)
      .add("behavior_type", StringType)
      .add("quantity", IntegerType)
      .add("amount", DoubleType)
      .add("timestamp", LongType)
      .add("session_id", StringType)

    // 从 Kafka 读取流数据，并解析 JSON
    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.32.100:9092")
      .option("subscribe", "user_behavior")
      .option("startingOffsets", "latest")
      .load()
      .select(from_json($"value".cast("string"), schema).alias("data"))
      .select("data.*")
      .withColumn("event_time", from_unixtime($"timestamp" / 1000).cast("timestamp"))

    // 添加水印（30分钟延迟，适配历史旧数据），进行5分钟滚动窗口（每1分钟滑动一次）聚合
    val windowed = rawDF
      .withWatermark("event_time", "30 minutes")
      .groupBy(window($"event_time", "5 minutes", "1 minute"))
      .agg(
        sum(when($"behavior_type" === "click", 1).otherwise(0)).alias("pv"),
        approx_count_distinct("user_id").alias("uv"),
        sum(when($"behavior_type" === "buy", $"amount").otherwise(0)).alias("sales")
      )
      .select($"window.start", $"window.end", $"pv", $"uv", $"sales")

    // 自定义写入 MySQL 的函数（使用 REPLACE INTO 避免主键重复）
    def writeToMySQL(df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long): Unit = {
      println(s"=== Processing batch $batchId with ${df.count()} rows ===")
      if (df.count() > 0) {
        df.show(10, false)

        // 使用 foreachPartition 批量写入，提高性能
        df.foreachPartition { partition =>
          if (partition.nonEmpty) {
            val url = "jdbc:mysql://192.168.32.100:3306/realtime_db?useSSL=false"
            val user = "root"
            val password = "123456"
            var connection: java.sql.Connection = null
            var stmt: java.sql.PreparedStatement = null
            try {
              connection = java.sql.DriverManager.getConnection(url, user, password)
              // 注意：要求 realtime_stats 表有唯一索引或主键 (start, end)
              stmt = connection.prepareStatement(
                """REPLACE INTO realtime_stats (start, end, pv, uv, sales)
                  |VALUES (?, ?, ?, ?, ?)""".stripMargin)
              partition.foreach { row =>
                stmt.setTimestamp(1, row.getAs[java.sql.Timestamp]("start"))
                stmt.setTimestamp(2, row.getAs[java.sql.Timestamp]("end"))
                stmt.setLong(3, row.getAs[Long]("pv"))
                stmt.setLong(4, row.getAs[Long]("uv"))
                stmt.setDouble(5, row.getAs[Double]("sales"))
                stmt.executeUpdate()
              }
            } finally {
              if (stmt != null) stmt.close()
              if (connection != null) connection.close()
            }
          }
        }
        println(s"Batch $batchId has been written to MySQL successfully.")
      } else {
        println(s"Batch $batchId is empty, nothing written.")
      }
    }

    // 启动流查询：使用 complete 模式（每个批次输出所有窗口），因为历史数据延迟较大，complete 模式能立即输出
    val query = windowed.writeStream
      .foreachBatch(writeToMySQL _)
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}



