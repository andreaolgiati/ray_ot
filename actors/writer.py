import ray
import random
import uuid
import time
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.prometheus import PrometheusMetricReader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@ray.remote
class Writer:
    def __init__(self, table_holder):
        # Initialize OTEL tracer
        trace.set_tracer_provider(TracerProvider())
        self.tracer = trace.get_tracer(__name__)
        span_processor = BatchSpanProcessor(PrometheusMetricReader())
        trace.get_tracer_provider().add_span_processor(span_processor)
        self.table_holder = table_holder
        self.total_requests = 0
        self.start_time = time.time()
        # Log initialization
        logging.info("Writer initialized")

    def add_row(self):
        with self.tracer.start_as_current_span("add_row"):
            # Log the current statement being executed
            logging.info("Executing add_row")
            # Generate random features and result
            features = [random.random() for _ in range(64)]
            result = random.random()

            # Add row to the table
            row_id = ray.get(self.table_holder.add_row.remote(features, result))

            # Increment total requests
            self.total_requests += 1

            # Deposit UUID into a set in the Ray object store
            ray.get(self.table_holder.uuids.add.remote(row_id))

    def run(self, rows_per_second):
        while True:
            start_time = time.time()
            for _ in range(rows_per_second):
                self.add_row()
            elapsed_time = time.time() - start_time
            time.sleep(max(0, 1 - elapsed_time))
            self.print_stats()
            # Log the current statement being executed
            logging.info("Executing run method")

    def print_stats(self):
        elapsed_time = time.time() - self.start_time
        requests_per_second = self.total_requests / elapsed_time
        # Log the current statement being executed along with the statistics
        logging.info(f"Executing print_stats: Total requests: {self.total_requests}, Requests per second: {requests_per_second:.2f}")
        print(f"Total requests: {self.total_requests}, Requests per second: {requests_per_second:.2f}")
