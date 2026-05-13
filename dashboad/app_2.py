# -*- coding: utf-8 -*-
import pymysql
from flask import Flask, render_template_string, jsonify, request

app = Flask(__name__)

db_config = {
    'host': '192.168.32.100',
    'user': 'root',
    'password': '123456',
    'database': 'realtime_db',
    'charset': 'utf8mb4'
}

def get_db_conn():
    return pymysql.connect(**db_config)

def get_realtime_stats(limit=30):
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT start, end, pv, uv, sales
        FROM realtime_stats
        ORDER BY start DESC
        LIMIT %s
    """, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def get_daily_reports(limit=30):
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT report_date, total_sales, top1_product, top1_sales,
               top2_product, top2_sales, top3_product, top3_sales,
               click_uv, cart_uv, buy_uv
        FROM daily_reports
        ORDER BY report_date DESC
        LIMIT %s
    """, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.route('/')
def dashboard():
    return render_template_string(REALTIME_TEMPLATE)

@app.route('/daily')
def daily():
    return render_template_string(DAILY_TEMPLATE)

@app.route('/api/realtime')
def api_realtime():
    rows = get_realtime_stats(30)
    rows_rev = rows[::-1]
    data = {
        'times': [f"{row[0].strftime('%H:%M')}-{row[1].strftime('%H:%M')}" for row in rows_rev],
        'pv': [row[2] for row in rows_rev],
        'uv': [row[3] for row in rows_rev],
        'sales': [row[4] for row in rows_rev]
    }
    return jsonify(data)

@app.route('/api/daily')
def api_daily():
    conn = get_db_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT report_date, total_sales, top1_product, top1_sales,
                   top2_product, top2_sales, top3_product, top3_sales,
                   click_uv, cart_uv, buy_uv
            FROM daily_reports
            ORDER BY report_date DESC
            LIMIT 30
        """)
        rows = cursor.fetchall()
        data = {
            'dates': [str(row[0]) for row in rows][::-1],
            'total_sales': [float(row[1]) for row in rows][::-1],
            'click_uv': [int(row[8]) for row in rows][::-1],
            'cart_uv': [int(row[9]) for row in rows][::-1],
            'buy_uv': [int(row[10]) for row in rows][::-1],
            'table_rows': [
                {
                    'date': str(row[0]),
                    'total_sales': float(row[1]),
                    'top1': f"{row[2]} ({float(row[3])})",
                    'top2': f"{row[4]} ({float(row[5])})",
                    'top3': f"{row[6]} ({float(row[7])})",
                    'click_uv': int(row[8]),
                    'cart_uv': int(row[9]),
                    'buy_uv': int(row[10])
                } for row in rows
            ][::-1]
        }
        return jsonify(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

REALTIME_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Realtime Dashboard | E-commerce Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
    <style>
        body { font-family: Arial; margin: 20px; background: #f0f2f5; }
        .container { max-width: 1400px; margin: auto; }
        .nav { margin-bottom: 20px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #2c3e50; font-weight: bold; }
        .nav a.active { color: #e67e22; border-bottom: 2px solid #e67e22; padding-bottom: 5px; }
        .chart { background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        h2 { margin-top: 0; color: #333; }
        .refresh-note { text-align: center; color: #7f8c8d; font-size: 12px; margin-top: 10px; }
    </style>
</head>
<body>
<div class="container">
    <div class="nav">
        <a href="/" class="active">Real-time</a>
        <a href="/daily">Daily Report</a>
    </div>
    <h1>Real-time User Behavior Dashboard</h1>
    <div class="chart">
        <h2>PV / UV Trend (last 30 windows)</h2>
        <div id="chart1" style="height: 400px;"></div>
    </div>
    <div class="chart">
        <h2>Sales Trend (Yuan)</h2>
        <div id="chart2" style="height: 400px;"></div>
    </div>
    <div class="refresh-note">Auto refresh every 10 seconds</div>
</div>
<script>
    let chart1, chart2;
    function initCharts() {
        chart1 = echarts.init(document.getElementById('chart1'));
        chart2 = echarts.init(document.getElementById('chart2'));
        chart1.setOption({
            tooltip: { trigger: 'axis' },
            legend: { data: ['PV', 'UV'] },
            xAxis: { type: 'category' },
            yAxis: { type: 'value', name: 'Count' }
        });
        chart2.setOption({
            tooltip: { trigger: 'axis' },
            xAxis: { type: 'category' },
            yAxis: { type: 'value', name: 'Sales (Yuan)' }
        });
    }
    function fetchData() {
        fetch('/api/realtime')
            .then(res => res.json())
            .then(data => {
                chart1.setOption({
                    xAxis: { data: data.times },
                    series: [
                        { name: 'PV', type: 'line', smooth: true, data: data.pv },
                        { name: 'UV', type: 'line', smooth: true, data: data.uv }
                    ]
                });
                chart2.setOption({
                    xAxis: { data: data.times },
                    series: [{ name: 'Sales', type: 'bar', data: data.sales }]
                });
            })
            .catch(err => console.error('Fetch error:', err));
    }
    initCharts();
    fetchData();
    setInterval(fetchData, 10000);
</script>
</body>
</html>
'''

DAILY_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Daily Report | E-commerce Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
    <style>
        body { font-family: Arial; margin: 20px; background: #f0f2f5; }
        .container { max-width: 1400px; margin: auto; }
        .nav { margin-bottom: 20px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #2c3e50; font-weight: bold; }
        .nav a.active { color: #e67e22; border-bottom: 2px solid #e67e22; padding-bottom: 5px; }
        .chart { background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        h2 { margin-top: 0; color: #333; }
    </style>
</head>
<body>
<div class="container">
    <div class="nav">
        <a href="/">Real-time</a>
        <a href="/daily" class="active">Daily Report</a>
    </div>
    <h1>Daily Sales Report</h1>
    <div class="chart">
        <h2>Total Sales Trend</h2>
        <div id="chartSales" style="height: 400px;"></div>
    </div>
    <div class="chart">
        <h2>User Behavior Funnel (last 7 days)</h2>
        <div id="chartFunnel" style="height: 400px;"></div>
    </div>
    <div class="chart">
        <h2>Detailed Daily Report</h2>
        <div id="tableContainer"></div>
    </div>
</div>
<script>
    let salesChart, funnelChart;
    function initCharts() {
        salesChart = echarts.init(document.getElementById('chartSales'));
        funnelChart = echarts.init(document.getElementById('chartFunnel'));
        salesChart.setOption({
            tooltip: { trigger: 'axis' },
            xAxis: { type: 'category', axisLabel: { rotate: 45 } },
            yAxis: { type: 'value', name: 'Sales (Yuan)' }
        });
        funnelChart.setOption({
            tooltip: { trigger: 'axis' },
            legend: { data: ['Click UV', 'Cart UV', 'Buy UV'] },
            xAxis: { type: 'category', axisLabel: { rotate: 30 } },
            yAxis: { type: 'value', name: 'User Count' }
        });
    }
    function renderTable(rows) {
        let html = '<table><thead><tr><th>Date</th><th>Total Sales</th><th>Top1 Product (Sales)</th><th>Top2 Product</th><th>Top3 Product</th><th>Click UV</th><th>Cart UV</th><th>Buy UV</th></tr></thead><tbody>';
        rows.forEach(row => {
            html += `<tr>
                        <td>${row.date}</td>
                        <td>${row.total_sales.toFixed(2)}</td>
                        <td>${row.top1}</td>
                        <td>${row.top2}</td>
                        <td>${row.top3}</td>
                        <td>${row.click_uv}</td>
                        <td>${row.cart_uv}</td>
                        <td>${row.buy_uv}</td>
                     </tr>`;
        });
        html += '</tbody></table>';
        document.getElementById('tableContainer').innerHTML = html;
    }
    function fetchData() {
        fetch('/api/daily')
            .then(res => res.json())
            .then(data => {
                salesChart.setOption({
                    xAxis: { data: data.dates },
                    series: [{ name: 'Total Sales', type: 'line', smooth: true, data: data.total_sales, areaStyle: {} }]
                });
                let last7Dates = data.dates.slice(-7);
                let last7Click = data.click_uv.slice(-7);
                let last7Cart = data.cart_uv.slice(-7);
                let last7Buy = data.buy_uv.slice(-7);
                funnelChart.setOption({
                    xAxis: { data: last7Dates },
                    series: [
                        { name: 'Click UV', type: 'bar', data: last7Click },
                        { name: 'Cart UV', type: 'bar', data: last7Cart },
                        { name: 'Buy UV', type: 'bar', data: last7Buy }
                    ]
                });
                renderTable(data.table_rows.slice(-7));
            })
            .catch(err => console.error('Fetch error:', err));
    }
    initCharts();
    fetchData();
    setInterval(fetchData, 30000);
</script>
</body>
</html>
'''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)