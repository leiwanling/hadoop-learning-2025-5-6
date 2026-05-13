#!/bin/bash
# kafka_producer.sh - 使用 Kafka 命令行工具生成模拟数据

KAFKA_HOME=/usr/local/kafka
TOPIC=user_behavior
BOOTSTRAP=192.168.32.100:9092

# 行为类型及其权重（click 70%, cart 15%, buy 5%, fav 10%）
behaviors=(
    "click" "click" "click" "click" "click" "click" "click"
    "cart" "cart" "cart"
    "buy" "buy"
    "fav" "fav" "fav" "fav"
)

while true; do
    # 随机选择行为
    idx=$((RANDOM % ${#behaviors[@]}))
    behavior=${behaviors[$idx]}

    # 生成随机 ID
    user_id="u_$((RANDOM % 10000 + 1))"
    item_id="i_$((RANDOM % 500 + 1))"
    category_id="c_$((RANDOM % 50 + 1))"
    session_id="s_$((RANDOM % 900000 + 100000))"
    timestamp=$(date +%s)000

    # 根据行为决定数量和金额
    if [ "$behavior" == "buy" ]; then
        quantity=$((RANDOM % 3 + 1))
        amount=$(echo "scale=2; $((RANDOM % 490 + 10)).$((RANDOM % 99))" | bc)
    else
        quantity=0
        amount=0
    fi

    # 构建 JSON
    json=$(printf '{"user_id":"%s","item_id":"%s","category_id":"%s","behavior_type":"%s","quantity":%d,"amount":%s,"timestamp":%d,"session_id":"%s"}' \
        "$user_id" "$item_id" "$category_id" "$behavior" "$quantity" "$amount" "$timestamp" "$session_id")

    # 发送到 Kafka
    echo "$json" | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $BOOTSTRAP --topic $TOPIC > /dev/null 2>&1

    # 控制速率：约 100 条/秒
    sleep 0.01
done