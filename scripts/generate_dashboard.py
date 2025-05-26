#!/usr/bin/env python3
# scripts/generate_dashboard.py

import os
import json
import pandas as pd
import requests
import glob
from datetime import datetime
import re

def get_github_data():
    """Get GitHub workflow data"""
    token = os.getenv('GITHUB_TOKEN')
    repo = os.getenv('GITHUB_REPOSITORY', 'unknown/repo')
    
    if not token:
        return None
        
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    try:
        # Get workflow runs
        url = f'https://api.github.com/repos/{repo}/actions/runs'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching GitHub data: {e}")
    
    return None

def analyze_current_data():
    """Analyze current data files"""
    stats = {
        'total_bus_stops': int(os.getenv('TOTAL_STOPS', 0)),
        'corrections_count': int(os.getenv('CORRECTIONS', 0)),
        'success_rate': float(os.getenv('SUCCESS_RATE', 0)),
        'last_update': datetime.now().strftime('%d/%m/%Y'),
        'changes_over_time': [],
        'recent_activities': []
    }
    
    try:
        # If environment variables are not set, try to read from files
        if stats['total_bus_stops'] == 0:
            correction_files = glob.glob('data/lta_correction*.csv')
            if correction_files:
                latest_file = max(correction_files, key=lambda x: os.path.getctime(x) if os.path.exists(x) else 0)
                if os.path.exists(latest_file):
                    df = pd.read_csv(latest_file)
                    stats['total_bus_stops'] = len(df)
                    stats['corrections_count'] = len(df[df['name_source'] == 'SimplyGo']) if 'name_source' in df.columns else 0
                    stats['success_rate'] = 100.0
        
        # Analyze historical data
        lta_files = glob.glob('data/LTA_bus_stops_*.csv')
        lta_files.sort()
        
        for file in lta_files[-6:]:  # Last 6 files
            if os.path.exists(file):
                try:
                    df = pd.read_csv(file)
                    filename = os.path.basename(file)
                    # Extract date from filename: LTA_bus_stops_16052025.csv
                    date_match = re.search(r'LTA_bus_stops_(\d{8})\.csv', filename)
                    if date_match:
                        date_str = date_match.group(1)
                        # Convert to readable format
                        formatted_date = f"{date_str[0:2]}/{date_str[2:4]}/{date_str[4:8]}"
                        
                        stats['changes_over_time'].append({
                            'date': formatted_date,
                            'count': len(df)
                        })
                except Exception as e:
                    print(f"Error reading file {file}: {e}")
        
        # Get recent activities from logs
        log_files = glob.glob('logs/bus_data_collector_*.log')
        if log_files:
            latest_log = max(log_files, key=lambda x: os.path.getctime(x) if os.path.exists(x) else 0)
            if os.path.exists(latest_log):
                try:
                    with open(latest_log, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                    
                    # Extract key activities
                    activities = []
                    for line in lines:
                        if 'INFO' in line and any(keyword in line for keyword in 
                            ['completed successfully', 'Downloaded', 'Found', 'Corrected', 'Saved']):
                            
                            try:
                                # Parse timestamp and message
                                parts = line.split(' - ')
                                if len(parts) >= 3:
                                    timestamp = parts[0].strip()
                                    message = parts[-1].strip()
                                    
                                    # Clean up message
                                    if 'completed successfully' in message:
                                        message = '✅ ' + message
                                        activity_type = 'success'
                                    elif 'Downloaded' in message:
                                        message = '📥 ' + message
                                        activity_type = 'info'
                                    elif 'Found' in message and ('new' in message or 'modified' in message):
                                        message = '🔄 ' + message
                                        activity_type = 'info'
                                    elif 'Corrected' in message:
                                        message = '🔧 ' + message
                                        activity_type = 'success'
                                    elif 'Saved' in message:
                                        message = '💾 ' + message
                                        activity_type = 'info'
                                    else:
                                        activity_type = 'info'
                                    
                                    activities.append({
                                        'timestamp': timestamp,
                                        'message': message,
                                        'type': activity_type
                                    })
                            except Exception as e:
                                print(f"Error parsing log line: {e}")
                                continue
                    
                    # Get last 5 activities
                    stats['recent_activities'] = activities[-5:] if activities else []
                    
                except Exception as e:
                    print(f"Error reading log file: {e}")
    
    except Exception as e:
        print(f"Error analyzing data: {e}")
    
    return stats

def create_dashboard_html(stats, github_data):
    """Create the dashboard HTML"""
    
    # Calculate percentages
    correction_pct = (stats['corrections_count'] / stats['total_bus_stops'] * 100) if stats['total_bus_stops'] > 0 else 0
    
    # Format activities HTML
    activities_html = ""
    for activity in stats['recent_activities']:
        status_class = activity.get('type', 'info')
        activities_html += f'''
        <div class="timeline-item {status_class}">
            <span class="timeline-time">{activity['timestamp']}</span>
            <span class="timeline-message">{activity['message']}</span>
        </div>
        '''
    
    if not activities_html:
        activities_html = '''
        <div class="timeline-item info">
            <span class="timeline-time">Just now</span>
            <span class="timeline-message">🔄 Dashboard updated</span>
        </div>
        '''
    
    # Format chart data
    chart_dates = [item['date'] for item in stats['changes_over_time']]
    chart_counts = [item['count'] for item in stats['changes_over_time']]
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bus Stop Data Monitoring Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            backdrop-filter: blur(10px);
        }}

        .header {{
            text-align: center;
            margin-bottom: 40px;
            padding: 20px;
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            border-radius: 15px;
            color: white;
        }}

        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
        }}

        .header p {{
            font-size: 1.2em;
            opacity: 0.9;
        }}

        .status-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}

        .status-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.1);
            border-top: 4px solid var(--accent-color);
            position: relative;
        }}

        .status-card.success {{ --accent-color: #4ade80; }}
        .status-card.warning {{ --accent-color: #fbbf24; }}
        .status-card.info {{ --accent-color: #3b82f6; }}
        .status-card.error {{ --accent-color: #ef4444; }}

        .status-card h3 {{
            color: #374151;
            margin-bottom: 15px;
            font-size: 1.1em;
        }}

        .status-value {{
            font-size: 2.5em;
            font-weight: bold;
            color: var(--accent-color);
            margin-bottom: 10px;
        }}

        .status-label {{
            color: #6b7280;
            font-size: 0.9em;
        }}

        .charts-section {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            margin-bottom: 40px;
        }}

        .chart-container {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.1);
        }}

        .chart-container h3 {{
            margin-bottom: 20px;
            color: #374151;
            text-align: center;
        }}

        .timeline-section {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.1);
            margin-bottom: 30px;
        }}

        .timeline-item {{
            display: flex;
            align-items: flex-start;
            padding: 15px;
            margin-bottom: 10px;
            background: #f8fafc;
            border-radius: 10px;
            border-left: 4px solid #3b82f6;
        }}

        .timeline-item.success {{ border-left-color: #4ade80; }}
        .timeline-item.error {{ border-left-color: #ef4444; }}
        .timeline-item.info {{ border-left-color: #3b82f6; }}

        .timeline-time {{
            font-weight: bold;
            color: #374151;
            margin-right: 15px;
            min-width: 160px;
            font-size: 0.9em;
        }}

        .timeline-message {{
            color: #6b7280;
            line-height: 1.4;
        }}

        .footer {{
            text-align: center;
            padding: 20px;
            color: #6b7280;
            font-size: 0.9em;
        }}

        @media (max-width: 768px) {{
            .charts-section {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 2em;
            }}
            
            .status-grid {{
                grid-template-columns: 1fr;
            }}
            
            .timeline-time {{
                min-width: 120px;
                font-size: 0.8em;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚌 Bus Stop Data Monitor</h1>
            <p>Real-time monitoring of LTA DataMall & SimplyGo integration</p>
        </div>

        <div class="status-grid">
            <div class="status-card success">
                <h3>Total Bus Stops</h3>
                <div class="status-value">{stats['total_bus_stops']:,}</div>
                <div class="status-label">Active bus stops</div>
            </div>

            <div class="status-card info">
                <h3>Last Update</h3>
                <div class="status-value" style="font-size: 1.2em;">{stats['last_update']}</div>
                <div class="status-label">Data freshness</div>
            </div>

            <div class="status-card warning">
                <h3>SimplyGo Corrections</h3>
                <div class="status-value">{stats['corrections_count']:,}</div>
                <div class="status-label">Names corrected ({correction_pct:.1f}%)</div>
            </div>

            <div class="status-card success">
                <h3>Success Rate</h3>
                <div class="status-value">{stats['success_rate']:.0f}%</div>
                <div class="status-label">Scraping success</div>
            </div>
        </div>

        <div class="charts-section">
            <div class="chart-container">
                <h3>Data Source Distribution</h3>
                <canvas id="sourceChart"></canvas>
            </div>

            <div class="chart-container">
                <h3>Bus Stops Over Time</h3>
                <canvas id="timelineChart"></canvas>
            </div>
        </div>

        <div class="timeline-section">
            <h3>Recent Activity</h3>
            <div id="activityTimeline">
                {activities_html}
            </div>
        </div>
        
        <div class="footer">
            <p>🔄 Last updated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} | 
            📊 Monitoring {stats['total_bus_stops']:,} bus stops across Singapore</p>
        </div>
    </div>

    <script>
        // Source Distribution Chart
        const sourceCtx = document.getElementById('sourceChart').getContext('2d');
        const sourceChart = new Chart(sourceCtx, {{
            type: 'doughnut',
            data: {{
                labels: ['SimplyGo Data', 'LTA Data'],
                datasets: [{{
                    data: [{stats['corrections_count']}, {stats['total_bus_stops'] - stats['corrections_count']}],
                    backgroundColor: ['#3b82f6', '#e5e7eb'],
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{
                            padding: 20,
                            usePointStyle: true
                        }}
                    }}
                }}
            }}
        }});

        // Timeline Chart
        const timelineCtx = document.getElementById('timelineChart').getContext('2d');
        const timelineChart = new Chart(timelineCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(chart_dates)},
                datasets: [{{
                    label: 'Total Bus Stops',
                    data: {json.dumps(chart_counts)},
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    tension: 0.4,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: false
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>'''
    
    return html_content

def main():
    print("🔄 Generating dashboard...")
    
    # Ensure dashboard directory exists
    os.makedirs('dashboard', exist_ok=True)
    
    # Get data
    github_data = get_github_data()
    stats = analyze_current_data()
    
    print(f"📊 Stats: {stats['total_bus_stops']} total, {stats['corrections_count']} corrections")
    
    # Create HTML dashboard
    html_content = create_dashboard_html(stats, github_data)
    
    # Write files
    with open('dashboard/index.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # Create a simple data.json for API access
    dashboard_data = {
        'updated_at': datetime.now().isoformat(),
        'stats': stats,
        'github_data': github_data
    }
    
    with open('dashboard/data.json', 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, indent=2)
    
    print("✅ Dashboard generated successfully!")
    print(f"📁 Files created in ./dashboard/")

if __name__ == '__main__':
    main()