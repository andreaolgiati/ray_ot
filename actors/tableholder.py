import ray
import duckdb
import uuid
import random
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.prometheus import PrometheusMetricReader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@ray.remote
class TableHolder:
    def __init__(self):
        # Initialize OTEL tracer
        trace.set_tracer_provider(TracerProvider())
        self.tracer = trace.get_tracer(__name__)
        span_processor = BatchSpanProcessor(PrometheusMetricReader())
        trace.get_tracer_provider().add_span_processor(span_processor)
        
        # Initialize the duckdb connection and create the table
        self.conn = duckdb.connect(database=':memory:')
        self.conn.execute('''
            CREATE TABLE EVENTS (
                ID UUID,
                CREATIONTIME TIMESTAMP,
                FEATURES DOUBLE[],
                RESULT DOUBLE,
                IMPRESSION BOOLEAN,
                IMPRESSIONTIME TIMESTAMP,
                ENGAGEMENT BOOLEAN,
                ENGAGEMENTTIME TIMESTAMP
            )
        ''')
        self.uuids = set()
        # Log initialization
        logging.info("TableHolder initialized")

    def add_row(self, features, result):
        with self.tracer.start_as_current_span("add_row"):
            # Log the current statement being executed
            logging.info("Executing add_row")
            # Add a row to the table with random features and NULL in the IMPRESSION and ENGAGEMENT columns
            row_id = uuid.uuid4()
            creation_time = duckdb.query('SELECT NOW()').fetchone()[0]
            self.conn.execute('''
                INSERT INTO EVENTS (ID, CREATIONTIME, FEATURES, RESULT, IMPRESSION, IMPRESSIONTIME, ENGAGEMENT, ENGAGEMENTTIME)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
            ''', (row_id, creation_time, features, result))
            self.uuids.add(row_id)
            return row_id

    def get_random_uuid(self):
        with self.tracer.start_as_current_span("get_random_uuid"):
            # Log the current statement being executed
            logging.info("Executing get_random_uuid")
            # Return a random UUID from the created rows
            return random.choice(list(self.uuids))

    def get_table(self):
        with self.tracer.start_as_current_span("get_table"):
            # Log the current statement being executed
            logging.info("Executing get_table")
            # Return the table
            return self.conn.table('EVENTS')

    def print_table_stats(self):
        with self.tracer.start_as_current_span("print_table_stats"):
            # Log the current statement being executed
            logging.info("Executing print_table_stats")
            # Get the number of rows in the table
            num_rows = self.conn.execute('SELECT COUNT(*) FROM EVENTS').fetchone()[0]
            # Get the number of statements in the table
            num_statements = self.conn.execute('SELECT COUNT(DISTINCT ID) FROM EVENTS').fetchone()[0]
            # Print the statistics
            logging.info(f"Table stats: Number of rows: {num_rows}, Number of statements: {num_statements}")
            print(f"Table stats: Number of rows: {num_rows}, Number of statements: {num_statements}")
