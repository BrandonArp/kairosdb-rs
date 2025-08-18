#!/usr/bin/env python3
"""
Demo Data Generator for KairosDB Rust Ingestion Service

This script generates realistic demo metrics and sends them to the KairosDB Rust ingestion service.
It simulates various types of metrics you might see in a real system:
- CPU usage metrics from multiple hosts
- Memory utilization
- Network throughput
- Application response times
- Error rates
"""

import json
import math
import random
import time
import requests
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any
import argparse

class MetricGenerator:
    def __init__(self, service_url: str = "http://localhost:8081"):
        self.service_url = service_url
        self.hosts = [f"web-{i:02d}" for i in range(1, 6)]  # web-01 to web-05
        self.apps = ["frontend", "api", "backend", "database"]
        self.running = False
        
    def generate_cpu_metrics(self) -> List[Dict[str, Any]]:
        """Generate CPU usage metrics for different hosts"""
        metrics = []
        current_time = int(time.time() * 1000)
        
        for host in self.hosts:
            # CPU usage oscillates around 40-60% with some randomness
            base_cpu = 50 + 10 * math.sin(time.time() / 120)  # 2-minute cycle
            cpu_usage = max(5, min(95, base_cpu + random.gauss(0, 8)))
            
            metrics.append({
                "name": "system.cpu.usage",
                "datapoints": [[current_time, round(cpu_usage, 2)]],
                "tags": {
                    "host": host,
                    "datacenter": "us-west-1",
                    "environment": "production"
                }
            })
        return metrics
    
    def generate_memory_metrics(self) -> List[Dict[str, Any]]:
        """Generate memory utilization metrics"""
        metrics = []
        current_time = int(time.time() * 1000)
        
        for host in self.hosts:
            # Memory usage gradually increases over time then drops (GC pattern)
            cycle_position = (time.time() % 300) / 300  # 5-minute cycle
            if cycle_position < 0.8:
                memory_usage = 30 + cycle_position * 50  # Gradual increase
            else:
                memory_usage = 80 - (cycle_position - 0.8) * 250  # Sharp drop (GC)
            
            memory_usage = max(20, min(90, memory_usage + random.gauss(0, 3)))
            
            metrics.append({
                "name": "system.memory.usage",
                "datapoints": [[current_time, round(memory_usage, 2)]],
                "tags": {
                    "host": host,
                    "datacenter": "us-west-1",
                    "environment": "production"
                }
            })
        return metrics
    
    def generate_network_metrics(self) -> List[Dict[str, Any]]:
        """Generate network throughput metrics"""
        metrics = []
        current_time = int(time.time() * 1000)
        
        for host in self.hosts:
            # Network traffic varies throughout the day
            hour = datetime.now().hour
            if 9 <= hour <= 17:  # Business hours
                base_throughput = 100 + 50 * math.sin(time.time() / 300)
            else:
                base_throughput = 20 + 10 * math.sin(time.time() / 600)
            
            rx_bytes = max(0, base_throughput + random.gauss(0, 10)) * 1024 * 1024  # MB/s to bytes/s
            tx_bytes = max(0, base_throughput * 0.6 + random.gauss(0, 5)) * 1024 * 1024
            
            metrics.extend([
                {
                    "name": "system.network.rx_bytes",
                    "datapoints": [[current_time, int(rx_bytes)]],
                    "tags": {
                        "host": host,
                        "interface": "eth0",
                        "datacenter": "us-west-1"
                    }
                },
                {
                    "name": "system.network.tx_bytes", 
                    "datapoints": [[current_time, int(tx_bytes)]],
                    "tags": {
                        "host": host,
                        "interface": "eth0",
                        "datacenter": "us-west-1"
                    }
                }
            ])
        return metrics
    
    def generate_application_metrics(self) -> List[Dict[str, Any]]:
        """Generate application-specific metrics"""
        metrics = []
        current_time = int(time.time() * 1000)
        
        for app in self.apps:
            for host in self.hosts:
                # Response time varies by application type
                if app == "database":
                    base_response_time = 5 + random.expovariate(1/2)  # Database queries
                elif app == "api":
                    base_response_time = 50 + random.expovariate(1/30)  # API calls
                else:
                    base_response_time = 100 + random.expovariate(1/50)  # Web requests
                
                response_time = max(1, base_response_time)
                
                # Error rate is usually low but spikes occasionally
                if random.random() < 0.05:  # 5% chance of error spike
                    error_rate = random.uniform(5, 15)
                else:
                    error_rate = random.uniform(0.1, 2.0)
                
                # Request rate varies by application
                if app == "frontend":
                    request_rate = random.uniform(100, 500)
                elif app == "api":
                    request_rate = random.uniform(50, 200)
                else:
                    request_rate = random.uniform(10, 100)
                
                metrics.extend([
                    {
                        "name": "application.response_time",
                        "datapoints": [[current_time, round(response_time, 2)]],
                        "tags": {
                            "application": app,
                            "host": host,
                            "datacenter": "us-west-1"
                        }
                    },
                    {
                        "name": "application.error_rate",
                        "datapoints": [[current_time, round(error_rate, 2)]],
                        "tags": {
                            "application": app,
                            "host": host,
                            "datacenter": "us-west-1"
                        }
                    },
                    {
                        "name": "application.request_rate",
                        "datapoints": [[current_time, round(request_rate, 2)]],
                        "tags": {
                            "application": app,
                            "host": host,
                            "datacenter": "us-west-1"
                        }
                    }
                ])
        return metrics
    
    def send_metrics_batch(self, metrics: List[Dict[str, Any]]) -> bool:
        """Send a batch of metrics to the ingestion service"""
        try:
            # Send each metric individually (KairosDB format expects one metric per request)
            for metric in metrics:
                response = requests.post(
                    f"{self.service_url}/api/v1/datapoints",
                    json=metric,
                    headers={"Content-Type": "application/json"},
                    timeout=5
                )
                if response.status_code != 200:
                    print(f"Warning: Failed to send metric {metric['name']}: {response.status_code}")
                    return False
            return True
        except Exception as e:
            print(f"Error sending metrics: {e}")
            return False
    
    def generate_and_send_metrics(self):
        """Generate and send all types of metrics"""
        try:
            all_metrics = []
            all_metrics.extend(self.generate_cpu_metrics())
            all_metrics.extend(self.generate_memory_metrics())
            all_metrics.extend(self.generate_network_metrics())
            all_metrics.extend(self.generate_application_metrics())
            
            success = self.send_metrics_batch(all_metrics)
            if success:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully sent {len(all_metrics)} metrics")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to send some metrics")
                
        except Exception as e:
            print(f"Error generating metrics: {e}")
    
    def start_continuous_generation(self, interval: int = 10):
        """Start generating metrics continuously"""
        print(f"Starting demo data generator...")
        print(f"Sending metrics to: {self.service_url}")
        print(f"Update interval: {interval} seconds")
        print(f"Press Ctrl+C to stop")
        
        self.running = True
        
        def worker():
            while self.running:
                self.generate_and_send_metrics()
                time.sleep(interval)
        
        try:
            worker_thread = threading.Thread(target=worker, daemon=True)
            worker_thread.start()
            
            # Keep main thread alive
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\nStopping demo data generator...")
            self.running = False

def main():
    parser = argparse.ArgumentParser(description="KairosDB Demo Data Generator")
    parser.add_argument("--url", default="http://localhost:8081", 
                       help="KairosDB ingestion service URL")
    parser.add_argument("--interval", type=float, default=10,
                       help="Interval between metric updates (seconds)")
    parser.add_argument("--once", action="store_true",
                       help="Send metrics once and exit (don't run continuously)")
    
    args = parser.parse_args()
    
    generator = MetricGenerator(args.url)
    
    if args.once:
        print("Generating single batch of demo metrics...")
        generator.generate_and_send_metrics()
        print("Done!")
    else:
        generator.start_continuous_generation(args.interval)

if __name__ == "__main__":
    main()