# -*- coding: utf-8 -*-
import mysql.connector
from flask import Flask, render_template_string, jsonify

app = Flask(__name__)

db_config = {
    'host': '192.168.32.100',
    'user': 'root',
    'password': '123456',
    'database': 'realtime_db'
}

def get_stats():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT start, end, pv, uv, sales
        FROM realtime_stats
        ORDER BY start DESC
        LIMIT 30
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)

@app.route('/data')
def data():
    rows = get_stats()
    data = {
        'times': [f"{row[0].strftime('%H:%M')}-{row[1].strftime('%H:%M')}" for row in rows][::-1],
        'pv': [row[2] for row in rows][::-1],
        'uv': [row[3] for row in rows][::-1],
        'sales': [row[4] for row in rows][::-1]
    }
    return jsonify(data)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Realtime Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
    <style>
        body { font-family: Arial; margin: 20px; background: #f0f2f5; }
        .container { max-width: 1400px; margin: auto; }
        .chart { background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        h2 { margin-top: 0; color: #333; }
    </style>
</head>
<body>
<div class="container">
    <h1>Real-time Dashboard</h1>
    <div class="chart">
        <h2>PV & UV</h2>
        <div id="chart1" style="height: 400px;"></div>
    </div>
    <div class="chart">
        <h2>Sales</h2>
        <div id="chart2" style="height: 400px;"></div>
    </div>
</div>
<script>
    fetch('/data')
        .then(response => response.json())
        .then(data => {
            var chart1 = echarts.init(document.getElementById('chart1'));
            chart1.setOption({
                tooltip: { trigger: 'axis' },
                legend: { data: ['PV', 'UV'] },
                xAxis: { type: 'category', data: data.times, axisLabel: { rotate: 45 } },
                yAxis: { type: 'value' },
                series: [
                    { name: 'PV', type: 'line', smooth: true, data: data.pv },
                    { name: 'UV', type: 'line', smooth: true, data: data.uv }
                ]
            });
            var chart2 = echarts.init(document.getElementById('chart2'));
            chart2.setOption({
                tooltip: { trigger: 'axis' },
                xAxis: { type: 'category', data: data.times, axisLabel: { rotate: 45 } },
                yAxis: { type: 'value', name: 'Sales' },
                series: [{ name: 'Sales', type: 'bar', data: data.sales }]
            });
        });
    setInterval(() => location.reload(), 15000);
</script>
</body>
</html>
'''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
