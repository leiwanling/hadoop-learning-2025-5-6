#!/bin/bash
# stop_all.sh - 停止所有组件

HA_NODES=("192.168.32.102" "192.168.32.103" "192.168.32.104")
PSEUDO_NODE="192.168.32.100"

echo "=== 停止所有服务 ==="

# 1. 停止 Flask 看板、Spark Streaming 作业、数据生产者
echo "停止伪分布式节点上的应用..."
ssh root@$PSEUDO_NODE "pkill -f 'python3 /root/dashboard/app.py'; pkill -f 'KafkaToMySQL'; pkill -f 'kafka_producer.py'"

# 2. 停止 Kafka
echo "停止 Kafka..."
ssh root@$PSEUDO_NODE "/usr/local/kafka/bin/kafka-server-stop.sh"

# 3. 停止 Flume
echo "停止 Flume..."
ssh root@192.168.32.102 "pkill -f flume"

# 4. 停止 HDFS 和 YARN
echo "停止 HDFS 和 YARN..."
ssh root@192.168.32.102 "/usr/local/hadoop/sbin/stop-dfs.sh"
ssh root@192.168.32.102 "/usr/local/hadoop/sbin/stop-yarn.sh"

# 5. 停止 ZooKeeper
echo "停止 ZooKeeper 集群..."
for node in "${HA_NODES[@]}"; do
    ssh root@$node "/usr/local/zookeeper/bin/zkServer.sh stop"
done

# 6. 停止 MySQL（可选）
# ssh root@$PSEUDO_NODE "systemctl stop mysqld"

echo "所有服务已停止."