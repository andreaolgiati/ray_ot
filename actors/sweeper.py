import ray
import time
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.prometheus import PrometheusMetricReader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@ray.remote
class Sweeper:
    def __init__(self, table_holder, expiration_minutes):
        
        # Initialize OTEL tracer
        trace.set_tracer_provider(TracerProvider())
        self.tracer = trace.get_tracer(__name__)
        span_processor = BatchSpanProcessor(PrometheusMetricReader())
        trace.get_tracer_provider().add_span_processor(span_processor)
        self.table_holder = table_holder
        self.expiration_minutes = expiration_minutes
        self.total_requests = 0
        self.start_time = time.time()
        # Log initialization
        logging.info("Sweeper initialized")

    def remove_old_rows(self):
        with self.tracer.start_as_current_span("remove_old_rows"):
            # Log the current statement being executed
            logging.info(f"Executing remove_old_rows with expiration_minutes={self.expiration_minutes}")
            # Remove rows older than the specified number of minutes
            self.table_holder.conn.execute('''
                DELETE FROM EVENTS
                WHERE CREATIONTIME < NOW() - INTERVAL ? MINUTE
            ''', (self.expiration_minutes,))

            # Increment total requests
            self.total_requests += 1

    def run(self):
        while True:
            self.remove_old_rows()
            time.sleep(60)  # Run every minute
            self.print_stats()
            # Log the current statement being executed
            logging.info("Executing run method")

    def print_stats(self):
        elapsed_time = time.time() - self.start_time
        requests_per_second = self.total_requests / elapsed_time
        # Log the current statement being executed along with the statistics
        logging.info(f"Executing print_stats: Total requests: {self.total_requests}, Requests per second: {requests_per_second:.2f}")
        print(f"Total requests: {self.total_requests}, Requests per second: {requests_per_second:.2f}")
