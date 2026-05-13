#!/bin/bash
# start_all.sh - 按顺序启动所有组件

set -e  # 遇到错误立即退出

# 定义节点 IP
HA_NODES=("192.168.32.102" "192.168.32.103" "192.168.32.104")
PSEUDO_NODE="192.168.32.100"

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== 启动电商行为分析系统 ===${NC}"

# 1. 在 HA 节点上启动 ZooKeeper
echo "1. 启动 ZooKeeper 集群..."
for node in "${HA_NODES[@]}"; do
    ssh root@$node "/usr/local/zookeeper/bin/zkServer.sh start"
    echo "  -> ZooKeeper 已启动 on $node"
done
sleep 5

# 2. 在 HA 节点上启动 HDFS (在 active NameNode 节点，假设 node01)
echo "2. 启动 HDFS HA 集群..."
ssh root@192.168.32.102 "/usr/local/hadoop/sbin/start-dfs.sh"
sleep 10

# 3. 在 HA 节点上启动 YARN
echo "3. 启动 YARN 集群..."
ssh root@192.168.32.102 "/usr/local/hadoop/sbin/start-yarn.sh"
sleep 5

# 4. 启动 HistoryServer (可选)
echo "4. 启动 MapReduce HistoryServer..."
ssh root@192.168.32.102 "/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver"

# 5. 在伪分布式节点上启动 Kafka
echo "5. 启动 Kafka..."
ssh root@$PSEUDO_NODE "/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties"
sleep 5

# 6. 在 HA 节点上启动 Flume (假设在 node01 上)
echo "6. 启动 Flume (消费 Kafka -> HDFS)..."
ssh root@192.168.32.102 "nohup /usr/local/flume/bin/flume-ng agent -n agent -c /usr/local/flume/conf -f /usr/local/flume/conf/kafka2hdfs.conf > /var/log/flume.log 2>&1 &"

# 7. 在伪分布式节点上启动 MySQL (如果未自启动)
echo "7. 启动 MySQL..."
ssh root@$PSEUDO_NODE "systemctl start mysqld"

# 8. 启动数据模拟生产者
echo "8. 启动 Kafka 数据生产者..."
ssh root@$PSEUDO_NODE "nohup python3 /root/kafka_producer.py > /dev/null 2>&1 &"

# 9. 提交 Spark Streaming 作业 (注意修改 jar 包路径和 MySQL 密码)
echo "9. 提交 Spark Streaming 实时作业..."
ssh root@$PSEUDO_NODE "nohup /spark/spark-yarn/bin/spark-submit --master local[*] --conf spark.hadoop.fs.defaultFS=hdfs://192.168.32.103:8020 --jars /root/mysql-connector-java-5.1.32-bin.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 --class KafkaToMySQL /root/rzxx.jar > /var/log/streaming.log 2>&1 &"

# 10. 启动 Flask 看板
echo "10. 启动 Flask 可视化看板..."
ssh root@$PSEUDO_NODE "nohup python3 /root/dashboard/app.py > /var/log/flask.log 2>&1 &"

echo -e "${GREEN}所有服务已启动完成！${NC}"
echo "查看实时看板: http://$PSEUDO_NODE:5000"