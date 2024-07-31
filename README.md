# ray_ot

## Setting up OTEL and Grafana

This project uses OpenTelemetry (OTEL) for logging and Grafana for visualizing the data. Follow the instructions below to set up OTEL and Grafana.

### Prerequisites

- Docker
- Docker Compose

### Steps

1. Clone the repository:

```sh
git clone https://github.com/andreaolgiati/ray_ot.git
cd ray_ot
```

2. Build and run the Docker containers:

```sh
docker-compose up --build
```

3. Access Grafana:

Open your web browser and go to `http://localhost:3000`. Use the default credentials (`admin`/`admin`) to log in.

4. Add Prometheus as a data source in Grafana:

- Go to `Configuration` > `Data Sources`.
- Click `Add data source`.
- Select `Prometheus`.
- Set the URL to `http://prometheus:9090`.
- Click `Save & Test`.

5. Import the dashboard:

- Go to `Create` > `Import`.
- Enter the dashboard ID or upload the JSON file provided in the `grafana` directory of this repository.
- Click `Load`.
- Select the Prometheus data source and click `Import`.

### Example Configuration

Here is an example configuration for OTEL and Grafana:

```yaml
version: '3.7'

services:
  otel-collector:
    image: otel/opentelemetry-collector:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports:
      - "4317:4317"
      - "55680:55680"

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-storage:/var/lib/grafana

volumes:
  grafana-storage:
```

### OTEL Collector Configuration

Create a file named `otel-collector-config.yaml` with the following content:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [prometheus]
```

### Prometheus Configuration

Create a file named `prometheus.yml` with the following content:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'otel-collector'
    static_configs:
      - targets: ['otel-collector:8889']
```

Now you have set up OTEL and Grafana to visualize the data from your application.
