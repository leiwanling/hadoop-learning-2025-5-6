二、详细部署指南
环境前提
硬件：4台虚拟机（3台 HA 节点 + 1台伪分布式节点），网络互通，已安装 CentOS 7 / RHEL 7+

软件版本：

Hadoop 3.3.4 (HDFS HA + YARN)

ZooKeeper 3.8.0

Kafka 3.3.1

Flume 1.9.0

Spark 3.3.1

MySQL 8.0

Python 3.6+

JDK 1.8

第一步：配置 HA 节点（node01, node02, node03）
1.1 配置 HDFS HA (QJM)
确保三个节点 /etc/hosts 包含所有主机名解析。

在 core-site.xml 中设置 fs.defaultFS 为 hdfs://mycluster。

在 hdfs-site.xml 中配置 dfs.nameservices=mycluster，以及两个 NameNode 的 RPC 地址，配置 dfs.ha.namenodes.mycluster=nn1,nn2。

配置 JournalNode 地址：dfs.namenode.shared.edits.dir=qjournal://node01:8485;node02:8485;node03:8485/mycluster。

配置自动故障转移：dfs.ha.automatic-failover.enabled=true，并指定 ZooKeeper 地址。

格式化 NameNode 并启动。

1.2 配置 ZooKeeper 集群
三个节点上安装 ZooKeeper，dataDir 配置为单独目录。

在 zoo.cfg 中配置 server.1=node01:2888:3888 等，并创建 myid 文件。

启动 ZK 服务：zkServer.sh start。

1.3 配置 YARN
在 yarn-site.xml 中指定 ResourceManager 地址（可以配置 RM HA，也可单点）。

启动 YARN 服务。

1.4 安装 Flume
解压 Flume，复制 Hadoop 配置文件 (core-site.xml, hdfs-site.xml) 到 Flume 的 conf 目录。

创建 Flume 配置文件 kafka2hdfs.conf（参考下文），启动 Flume。

第二步：配置伪分布式节点（node1, IP 192.168.32.100）
2.1 安装 Kafka (单节点)
下载 Kafka，修改 config/server.properties：

properties
broker.id=0
listeners=PLAINTEXT://192.168.32.100:9092
advertised.listeners=PLAINTEXT://192.168.32.100:9092
log.dirs=/data/kafka-logs
zookeeper.connect=192.168.32.102:2181,192.168.32.103:2181,192.168.32.104:2181
启动 Kafka：kafka-server-start.sh -daemon config/server.properties

创建 topic：kafka-topics.sh --create --bootstrap-server 192.168.32.100:9092 --topic user_behavior --partitions 3 --replication-factor 1

2.2 安装 MySQL
安装 MySQL 8.0，设置 root 密码。

创建数据库 realtime_db 和两张表：

sql
CREATE TABLE realtime_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pv BIGINT,
    uv BIGINT,
    sales DOUBLE
);
CREATE TABLE daily_reports (
    report_date DATE PRIMARY KEY,
    total_sales DOUBLE,
    top1_product VARCHAR(50),
    top1_sales DOUBLE,
    top2_product VARCHAR(50),
    top2_sales DOUBLE,
    top3_product VARCHAR(50),
    top3_sales DOUBLE,
    click_uv BIGINT,
    cart_uv BIGINT,
    buy_uv BIGINT
);
2.3 安装 Spark (local 模式)
解压 Spark，配置环境变量。

将 Hadoop 配置文件复制到 Spark 的 conf 目录，以便识别 HDFS HA。

2.4 准备模拟数据生产者
安装 Python 依赖：pip3 install kafka-python

编写 kafka_producer.py 脚本（持续生成随机 JSON），后台运行。

2.5 提交 Spark Streaming 作业
bash
spark-submit --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://192.168.32.103:8020 \
  --jars mysql-connector-java-5.1.32-bin.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  --class KafkaToMySQL \
  /path/to/streaming.jar
注意：hdfs://192.168.32.103:8020 应为当前 active NameNode 地址（可用 hdfs haadmin -getServiceState nn1 查看）。

2.6 提交离线 Spark SQL 作业（每日一次）
bash
spark-submit --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://192.168.32.103:8020 \
  --jars mysql-connector-java-5.1.32-bin.jar \
  --class DailyOfflineAnalysis \
  /path/to/offline.jar 2026-05-13
可以配置 crontab 每天凌晨自动执行。   这里的2026-05-13根据hdfs上的时间修改

2.7 部署 Flask 可视化
安装 flask, pymysql。

将 app.py 放在目录中，执行 python3 app.py。

确保防火墙开放 5000 端口（或使用 0.0.0.0 绑定）。

第三步：启动顺序
启动 ZooKeeper (三台 HA 节点)

启动 HDFS (node01: start-dfs.sh)

启动 YARN (node01: start-yarn.sh)

启动 Kafka (伪分布式节点)

启动 Flume (任意一台 HA 节点)

启动数据生产者 (伪分布式节点)

提交 Spark Streaming 作业

启动 Flask 看板

（可选）执行离线作业

第四步：验证
查看 HDFS 上是否有 Flume 写入的文件：hdfs dfs -ls /data/original/behavior/dt=...

查看 MySQL realtime_stats 表是否有持续写入。

访问 http://192.168.32.100:5000 查看图表。