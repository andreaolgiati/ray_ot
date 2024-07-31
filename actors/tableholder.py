import ray
import duckdb
import uuid
import random

@ray.remote
class TableHolder:
    def __init__(self):
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

    def add_row(self, features, result):
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
        # Return a random UUID from the created rows
        return random.choice(list(self.uuids))

    def get_table(self):
        # Return the table
        return self.conn.table('EVENTS')
