import ray
import random
import time
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.prometheus import PrometheusMetricsExporter

# Initialize OTEL tracer
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
span_processor = BatchSpanProcessor(PrometheusMetricsExporter())
trace.get_tracer_provider().add_span_processor(span_processor)

@ray.remote
class Updater:
    def __init__(self, table_holder):
        self.table_holder = table_holder
        self.total_requests = 0
        self.start_time = time.time()

    def update_row(self):
        with tracer.start_as_current_span("update_row"):
            # Get a random UUID from the table holder
            row_id = ray.get(self.table_holder.get_random_uuid.remote())

            # Generate random IMPRESSION and IMPRESSIONTIME values
            impression = True
            impression_time = duckdb.query('SELECT NOW()').fetchone()[0]

            # Optionally generate ENGAGEMENT and ENGAGEMENTTIME values
            engagement = None
            engagement_time = None
            if random.random() < 0.5:  # 50% probability
                engagement = True
                engagement_time = duckdb.query('SELECT NOW()').fetchone()[0]

            # Update the row in the table
            self.table_holder.conn.execute('''
                UPDATE EVENTS
                SET IMPRESSION = ?, IMPRESSIONTIME = ?, ENGAGEMENT = ?, ENGAGEMENTTIME = ?
                WHERE ID = ?
            ''', (impression, impression_time, engagement, engagement_time, row_id))

            # Increment total requests
            self.total_requests += 1

    def run(self, rows_per_second):
        while True:
            start_time = time.time()
            for _ in range(rows_per_second):
                self.update_row()
            elapsed_time = time.time() - start_time
            time.sleep(max(0, 1 - elapsed_time))
            self.print_stats()

    def print_stats(self):
        elapsed_time = time.time() - self.start_time
        requests_per_second = self.total_requests / elapsed_time
        print(f"Total requests: {self.total_requests}, Requests per second: {requests_per_second:.2f}")
