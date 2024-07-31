import ray
import time

@ray.remote
class Sweeper:
    def __init__(self, table_holder, expiration_minutes):
        self.table_holder = table_holder
        self.expiration_minutes = expiration_minutes
        self.total_requests = 0
        self.start_time = time.time()

    def remove_old_rows(self):
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

    def print_stats(self):
        elapsed_time = time.time() - self.start_time
        requests_per_second = self.total_requests / elapsed_time
        print(f"Total requests: {self.total_requests}, Requests per second: {requests_per_second:.2f}")
