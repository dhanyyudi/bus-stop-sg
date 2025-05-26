#!/usr/bin/env python3
# scripts/send_slack_notification.py

import os
import json
import requests
from datetime import datetime

def send_slack_notification():
    """Send enhanced Slack notification"""
    
    webhook_url = os.getenv('SLACK_WEBHOOK')
    if not webhook_url:
        print("❌ SLACK_WEBHOOK not found in environment variables")
        return False
    
    # Get data from environment variables (set by GitHub Actions)
    total_stops = os.getenv('TOTAL_STOPS', '0')
    corrections = os.getenv('CORRECTIONS', '0')
    success_rate = os.getenv('SUCCESS_RATE', '0')
    dashboard_url = os.getenv('DASHBOARD_URL', 'https://github.com')
    workflow_status = os.getenv('WORKFLOW_STATUS', 'unknown')
    
    # Determine message color based on success rate
    if float(success_rate) >= 95:
        color = "good"  # Green
        status_emoji = "✅"
    elif float(success_rate) >= 80:
        color = "warning"  # Yellow
        status_emoji = "⚠️"
    else:
        color = "danger"  # Red
        status_emoji = "❌"
    
    # Calculate correction percentage
    correction_pct = 0
    if int(total_stops) > 0:
        correction_pct = (int(corrections) / int(total_stops)) * 100
    
    # Create rich Slack message
    message = {
        "channel": "#bus-stop-alerts",
        "username": "Bus Stop Monitor",
        "icon_emoji": ":bus:",
        "text": f"{status_emoji} Bus Stop Data Collection Completed",
        "attachments": [
            {
                "color": color,
                "title": "📊 Data Collection Summary",
                "title_link": dashboard_url,
                "fields": [
                    {
                        "title": "Total Bus Stops",
                        "value": f"{int(total_stops):,}",
                        "short": True
                    },
                    {
                        "title": "Success Rate",
                        "value": f"{float(success_rate):.1f}%",
                        "short": True
                    },
                    {
                        "title": "Corrections Applied",
                        "value": f"{int(corrections):,}",
                        "short": True
                    },
                    {
                        "title": "Correction Rate",
                        "value": f"{correction_pct:.1f}%",
                        "short": True
                    }
                ],
                "footer": "Bus Stop Monitor",
                "footer_icon": "https://github.githubassets.com/favicons/favicon.svg",
                "ts": int(datetime.now().timestamp())
            }
        ],
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{status_emoji} Bus Stop Data Collection Completed*\n\n📊 Processed *{int(total_stops):,}* bus stops with *{float(success_rate):.1f}%* success rate\n🔄 Applied *{int(corrections):,}* name corrections from SimplyGo"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📈 View Dashboard",
                            "emoji": True
                        },
                        "url": dashboard_url,
                        "style": "primary"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📋 View Logs",
                            "emoji": True
                        },
                        "url": f"https://github.com/{os.getenv('GITHUB_REPOSITORY', 'unknown/repo')}/actions"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} | 🤖 Automated via GitHub Actions"
                    }
                ]
            }
        ]
    }
    
    try:
        # Send to Slack
        response = requests.post(
            webhook_url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(message),
            timeout=30
        )
        
        if response.status_code == 200:
            print("✅ Slack notification sent successfully")
            return True
        else:
            print(f"❌ Failed to send Slack notification: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error sending Slack notification: {str(e)}")
        return False

def send_error_notification(error_message):
    """Send error notification to Slack"""
    
    webhook_url = os.getenv('SLACK_WEBHOOK')
    if not webhook_url:
        return False
    
    message = {
        "channel": "#bus-stop-alerts",
        "username": "Bus Stop Monitor",
        "icon_emoji": ":rotating_light:",
        "text": "🚨 Bus Stop Data Collection Failed",
        "attachments": [
            {
                "color": "danger",
                "title": "❌ Workflow Error",
                "text": error_message,
                "footer": "Bus Stop Monitor",
                "ts": int(datetime.now().timestamp())
            }
        ],
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🚨 Bus Stop Data Collection Failed*\n\n```{error_message}```"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🔍 Check Logs",
                            "emoji": True
                        },
                        "url": f"https://github.com/{os.getenv('GITHUB_REPOSITORY', 'unknown/repo')}/actions",
                        "style": "danger"
                    }
                ]
            }
        ]
    }
    
    try:
        response = requests.post(webhook_url, json=message, timeout=30)
        return response.status_code == 200
    except:
        return False

def send_start_notification():
    """Send workflow start notification"""
    
    webhook_url = os.getenv('SLACK_WEBHOOK')
    if not webhook_url:
        return False
    
    message = {
        "channel": "#bus-stop-alerts",
        "username": "Bus Stop Monitor", 
        "icon_emoji": ":hourglass_flowing_sand:",
        "text": "🔄 Bus Stop Data Collection Started",
        "attachments": [
            {
                "color": "#36a64f",
                "title": "🔄 Data Collection In Progress",
                "text": "Downloading LTA DataMall data and scraping SimplyGo corrections...",
                "footer": "Bus Stop Monitor",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }
    
    try:
        response = requests.post(webhook_url, json=message, timeout=30)
        return response.status_code == 200
    except:
        return False

if __name__ == '__main__':
    # Test the notification
    os.environ['TOTAL_STOPS'] = '5170'
    os.environ['CORRECTIONS'] = '1835'
    os.environ['SUCCESS_RATE'] = '100.0'
    os.environ['DASHBOARD_URL'] = 'https://example.github.io/bus-stop-data/'
    
    result = send_slack_notification()
    if result:
        print("✅ Test notification sent successfully!")
    else:
        print("❌ Test notification failed!")