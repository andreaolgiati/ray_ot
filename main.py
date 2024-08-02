import ray
import time
import logging
from constants import NUM_READERS, NUM_WRITERS, NUM_UPDATERS, ROWS_READ_PER_SECOND, ROWS_WRITTEN_PER_SECOND, ROWS_UPDATED_PER_SECOND, ROW_EXPIRATION_MINUTES
from actors.tableholder import TableHolder
from actors.writer import Writer
from actors.updater import Updater
from actors.reader import Reader
from actors.sweeper import Sweeper
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.prometheus import PrometheusMetricReader

# Initialize OTEL tracer
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
span_processor = BatchSpanProcessor(PrometheusMetricReader())
trace.get_tracer_provider().add_span_processor(span_processor)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Ray
ray.init()

# Start the TABLEHOLDER actor
table_holder = TableHolder.remote()

# Start the WRITERS actors
writers = [Writer.remote(table_holder) for _ in range(NUM_WRITERS)]

# Start the UPDATERS actors
updaters = [Updater.remote(table_holder) for _ in range(NUM_UPDATERS)]

# Start the READERS actors
readers = [Reader.remote(table_holder) for _ in range(NUM_READERS)]

# Start the SWEEPER actor
sweeper = Sweeper.remote(table_holder, ROW_EXPIRATION_MINUTES)

# Add comments to explain each step
# The TABLEHOLDER actor holds the duckdb table and provides a utility function to return a random UUID from the created rows.
# The WRITERS actors add rows to the table with random features and NULL in the IMPRESSION and ENGAGEMENT columns, and deposit UUIDs into a set in the Ray object store.
# The UPDATERS actors update random rows with IMPRESSION and IMPRESSIONTIME values, and optionally ENGAGEMENT and ENGAGEMENTTIME values.
# The READERS actors select rows where IMPRESSION is NULL and where IMPRESSION is not NULL, based on user-specified values.
# The SWEEPER actor removes rows older than a user-specified number of minutes.

# Print general stats before the loop
print("General stats:")
print(f"Number of writers: {NUM_WRITERS}")
print(f"Number of updaters: {NUM_UPDATERS}")
print(f"Number of readers: {NUM_READERS}")
print(f"Rows read per second: {ROWS_READ_PER_SECOND}")
print(f"Rows written per second: {ROWS_WRITTEN_PER_SECOND}")
print(f"Rows updated per second: {ROWS_UPDATED_PER_SECOND}")
print(f"Row expiration minutes: {ROW_EXPIRATION_MINUTES}")

# Log the general stats
logging.info(f"General stats: Number of writers: {NUM_WRITERS}, Number of updaters: {NUM_UPDATERS}, Number of readers: {NUM_READERS}, Rows read per second: {ROWS_READ_PER_SECOND}, Rows written per second: {ROWS_WRITTEN_PER_SECOND}, Rows updated per second: {ROWS_UPDATED_PER_SECOND}, Row expiration minutes: {ROW_EXPIRATION_MINUTES}")

# Add an infinite loop to print general stats
while True:
    # Log the current statement being executed
    logging.info("Executing main loop: Printing general stats")
    
    # Call the print_table_stats method of the table holder
    ray.get(table_holder.print_table_stats.remote())
    
    time.sleep(10)  # Print stats every 10 seconds
