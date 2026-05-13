#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
import random
import time
import sys

MAX_BYTES = 1 * 1024 * 1024 * 1024   
bootstrap_servers = '192.168.32.100:9092'
topic = 'user_behavior'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

behaviors = ['click']*70 + ['cart']*15 + ['buy']*5 + ['fav']*10

total_bytes = 0
msg_count = 0

while total_bytes < MAX_BYTES:
    behavior = random.choice(behaviors)
    record = {
        'user_id': f"u_{random.randint(1,10000)}",
        'item_id': f"i_{random.randint(1,500)}",
        'category_id': f"c_{random.randint(1,50)}",
        'behavior_type': behavior,
        'quantity': random.randint(1,3) if behavior=='buy' else 0,
        'amount': round(random.uniform(10,500),2) if behavior=='buy' else 0.0,
        'timestamp': int(time.time()*1000),
        'session_id': f"s_{random.randint(100000,999999)}"
    }

    # 计算消息序列化后的大小（用于限制总字节数）
    serialized = json.dumps(record).encode('utf-8')
    msg_bytes = len(serialized)

    if total_bytes + msg_bytes > MAX_BYTES:
        break

    # 直接发送字典，value_serializer 会自动处理
    producer.send(topic, value=record)
    total_bytes += msg_bytes
    msg_count += 1

    time.sleep(0.01)

producer.flush()
producer.close()

print(f"已发送 {msg_count} 条消息，总字节数约 {total_bytes} ({total_bytes/1024/1024:.2f} MiB)")