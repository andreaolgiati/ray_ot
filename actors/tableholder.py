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
        logging.info("""
        ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____ 
       ||T ||||a ||||b ||||l ||||e ||||H ||||o ||||l ||||d ||||e ||||r ||||  ||||i ||||n ||||i ||||t ||||i ||
       ||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||
       |/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\|
        """)

    def add_row(self, features, result):
        with self.tracer.start_as_current_span("add_row"):
            # Log the current statement being executed
            logging.info("Executing add_row")
            logging.info("""
            ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____ 
           ||A ||||d ||||d ||||i ||||n ||||g ||||  ||||a ||||  ||||r ||||o ||||w ||||  ||||  ||||  ||||  ||
           ||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||
           |/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\|
            """)
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
            logging.info("""
            ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____ 
           ||G ||||e ||||t ||||t ||||i ||||n ||||g ||||  ||||a ||||  ||||r ||||a ||||n ||||d ||||o ||||m ||||  ||||U ||||U ||||I ||||D ||
           ||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||
           |/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\|
            """)
            # Return a random UUID from the created rows
            return random.choice(list(self.uuids))

    def get_table(self):
        with self.tracer.start_as_current_span("get_table"):
            # Log the current statement being executed
            logging.info("Executing get_table")
            logging.info("""
            ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____  ____ 
           ||G ||||e ||||t ||||t ||||i ||||n ||||g ||||  ||||t ||||h ||||e ||||  ||||t ||||a ||||b ||||l ||||e ||
           ||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||||__||
           |/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\||/__\|
            """)
            # Return the table
            return self.conn.table('EVENTS')
