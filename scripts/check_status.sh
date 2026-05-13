#!/bin/bash
# check_status.sh - 检查各组件运行状态

HA_NODES=("192.168.32.102" "192.168.32.103" "192.168.32.104")
PSEUDO_NODE="192.168.32.100"

echo "=== 组件状态检查 ==="

# 1. ZooKeeper
echo -e "\n[ZooKeeper]"
for node in "${HA_NODES[@]}"; do
    result=$(ssh root@$node "echo stat | nc 127.0.0.1 2181 | grep Mode" 2>/dev/null)
    if [ -n "$result" ]; then
        echo "  $node: $result"
    else
        echo "  $node: 未运行"
    fi
done

# 2. HDFS NameNode
echo -e "\n[HDFS NameNode]"
for node in "${HA_NODES[@]}"; do
    status=$(ssh root@$node "curl -s http://$node:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus | grep -oP '\"State\" : \"\K[^\"]+'")
    if [ "$status" = "active" ] || [ "$status" = "standby" ]; then
        echo "  $node: $status"
    else
        echo "  $node: 未响应"
    fi
done

# 3. DataNode (检查所有节点)
echo -e "\n[HDFS DataNode]"
for node in "${HA_NODES[@]}"; do
    count=$(ssh root@$node "jps | grep DataNode | wc -l")
    if [ $count -eq 1 ]; then
        echo "  $node: DataNode 运行中"
    else
        echo "  $node: DataNode 未运行"
    fi
done

# 4. YARN ResourceManager
echo -e "\n[YARN ResourceManager]"
rm_status=$(ssh root@192.168.32.102 "curl -s http://192.168.32.102:8088/ws/v1/cluster/info | grep -o 'HAState\" : \"[^\"]*' | cut -d'\"' -f4")
if [ -n "$rm_status" ]; then
    echo "  ResourceManager (102): $rm_status"
else
    echo "  ResourceManager (102): 未运行"
fi

# 5. Kafka
echo -e "\n[Kafka]"
kafka_pid=$(ssh root@$PSEUDO_NODE "ps aux | grep -v grep | grep kafka | awk '{print \$2}'")
if [ -n "$kafka_pid" ]; then
    echo "  Kafka (100): 运行中 PID=$kafka_pid"
else
    echo "  Kafka (100): 未运行"
fi

# 6. Flume
echo -e "\n[Flume]"
flume_pid=$(ssh root@192.168.32.102 "ps aux | grep -v grep | grep flume | awk '{print \$2}'")
if [ -n "$flume_pid" ]; then
    echo "  Flume (102): 运行中 PID=$flume_pid"
else
    echo "  Flume (102): 未运行"
fi

# 7. Spark Streaming 作业
echo -e "\n[Spark Streaming]"
spark_pid=$(ssh root@$PSEUDO_NODE "ps aux | grep -v grep | grep KafkaToMySQL | awk '{print \$2}'")
if [ -n "$spark_pid" ]; then
    echo "  Spark Streaming: 运行中 PID=$spark_pid"
else
    echo "  Spark Streaming: 未运行"
fi

# 8. MySQL
echo -e "\n[MySQL]"
mysql_status=$(ssh root@$PSEUDO_NODE "systemctl is-active mysqld" 2>/dev/null)
if [ "$mysql_status" = "active" ]; then
    echo "  MySQL (100): 运行中"
else
    echo "  MySQL (100): 未运行"
fi

# 9. Flask 看板
echo -e "\n[Flask Dashboard]"
flask_pid=$(ssh root@$PSEUDO_NODE "ps aux | grep -v grep | grep 'app.py' | awk '{print \$2}'")
if [ -n "$flask_pid" ]; then
    echo "  Flask 看板 (100): 运行中 PID=$flask_pid 端口5000"
else
    echo "  Flask 看板 (100): 未运行"
fi

# 10. 数据生产者
echo -e "\n[数据生产者]"
producer_pid=$(ssh root@$PSEUDO_NODE "ps aux | grep -v grep | grep kafka_producer.py | awk '{print \$2}'")
if [ -n "$producer_pid" ]; then
    echo "  生产者 (100): 运行中 PID=$producer_pid"
else
    echo "  生产者 (100): 未运行"
fi

echo -e "\n检查完成。"