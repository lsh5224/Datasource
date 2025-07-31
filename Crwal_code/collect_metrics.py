#!/usr/bin/env python3
import requests
import pandas as pd
from datetime import datetime
import os
import subprocess

def get_prometheus_url():
    try:
        # ingress에서 prometheus-ingress 도메인 추출
        output = subprocess.check_output([
            "kubectl", "get", "ingress", "-A",
            "-o", "jsonpath={.items[?(@.metadata.name=='prometheus-ingress')].status.loadBalancer.ingress[0].hostname}"
        ], universal_newlines=True)

        hostname = output.strip()
        if hostname:
            return f"http://{hostname}/api/v1/query"
    except Exception as e:
        print("[ERROR] Failed to retrieve Prometheus ingress:", e)
    return None

# Prometheus URL 자동 설정
PROM_URL = get_prometheus_url()
if PROM_URL is None:
    print("❌ Prometheus URL을 찾을 수 없습니다. 종료합니다.")
    exit(1)

# 쿼리 목록
queries = {
    "cpu_usage": 'sum by (pod) (rate(container_cpu_usage_seconds_total{container!="", pod!=""}[5m]))',
    "memory_usage": 'sum by (pod) (container_memory_usage_bytes{container!="", pod!=""})',
    "pod_pending": 'count by (pod) (kube_pod_status_phase{phase="Pending"})',
    "pod_restart": 'sum by (pod) (rate(kube_pod_container_status_restarts_total{pod!=""}[5m]))',
    "cpu_avg": 'avg by (pod) (rate(container_cpu_usage_seconds_total{container!="", pod!=""}[5m]))',
    "net_receive": 'sum by (pod) (rate(container_network_receive_bytes_total{pod!=""}[5m]))',
    "net_transmit": 'sum by (pod) (rate(container_network_transmit_bytes_total{pod!=""}[5m]))',
}

# 현재 시간 (고유값)
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
records = []

# 쿼리 실행
for metric, query in queries.items():
    try:
        res = requests.get(PROM_URL, params={"query": query}, timeout=10)
        res.raise_for_status()
        results = res.json().get("data", {}).get("result", [])

        for item in results:
            pod = item["metric"].get("pod", "unknown")
            value = item["value"][1]
            records.append([timestamp, pod, metric, value])

    except Exception as e:
        print(f"[ERROR] Failed to fetch '{metric}':", str(e))

# 결과 저장
df = pd.DataFrame(records, columns=["timestamp", "pod", "metric", "value"])
output_path = "/home/ubuntu/Datasource/Metric.tsv"
df.to_csv(output_path, sep="\t", index=False)
print(f"[OK] Saved: {output_path}")

# 데이터 병합
base_path = "./Datasource/"
new_file = base_path + "Metric.tsv"
existing_file = base_path + "Prometheus_data_set.tsv"

# TSV 파일 읽기
df_new = pd.read_csv(new_file, sep="\t")
df_existing = pd.read_csv(existing_file, sep="\t")

# 병합 및 중복 제거
df_combined = pd.concat([df_existing, df_new])
df_combined = df_combined.drop_duplicates(subset=["timestamp", "pod", "metric"], keep="last")
df_combined = df_combined.sort_values(by="timestamp")

# 덮어쓰기 저장
df_combined.to_csv(existing_file, sep="\t", index=False)

# Metric.tsv 삭제
if os.path.exists(new_file):
    os.remove(new_file)
    print("✅ 병합 완료 및 Metric.tsv 삭제됨")
else:
    print("⚠️ Metric.tsv 파일이 존재하지 않습니다")
