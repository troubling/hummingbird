# Dev Metrics Monitoring

Quick note: You don't have to install Prometheus for really basic dev monitoring. Just doing a curl request against the /metrics path for any service will give you all the data for that moment.

If you want to monitor the backend servers, you'll need to change their bind_ip settings to be reachable from the prometheus docker container.

Create ~/prometheus.yml replacing 192.168.56.104 with your server's location.

```
global:
  scrape_interval: 15s
  scrape_timeout: 10s
  evaluation_interval: 15s
  external_labels:
    monitor: codelab-monitor
scrape_configs:
  - job_name: prometheus
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - localhost:9090
  - job_name: proxy
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:8080
  - job_name: acount1
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6012
  - job_name: acount2
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6022
  - job_name: acount3
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6032
  - job_name: acount4
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6042
  - job_name: container1
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6011
  - job_name: container2
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6021
  - job_name: container3
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6031
  - job_name: container4
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6041
  - job_name: object1
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6010
  - job_name: object2
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6020
  - job_name: object3
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6030
  - job_name: object4
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    scheme: http
    static_configs:
      - targets:
        - 192.168.56.104:6040
```

Then, run a new Prometheus container:

```
docker run -d -p 0.0.0.0:9090:9090 -v ~/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus -config.file=/etc/prometheus/prometheus.yml -storage.local.path=/prometheus
``` 
