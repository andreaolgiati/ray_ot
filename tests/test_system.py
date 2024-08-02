import pytest
import ray
from unittest.mock import Mock, patch
from actors.tableholder import TableHolder
from actors.writer import Writer
from actors.updater import Updater
from actors.reader import Reader
from actors.sweeper import Sweeper
from constants import NUM_READERS, NUM_WRITERS, NUM_UPDATERS, ROWS_READ_PER_SECOND, ROWS_WRITTEN_PER_SECOND, ROWS_UPDATED_PER_SECOND, ROW_EXPIRATION_MINUTES

@pytest.fixture(scope="module")
def ray_init():
    ray.init()
    yield
    ray.shutdown()

def test_system(ray_init):
    # Mock the table holder
    table_holder = Mock(spec=TableHolder)

    # Initialize actors
    writers = [Writer.remote(table_holder) for _ in range(NUM_WRITERS)]
    updaters = [Updater.remote(table_holder) for _ in range(NUM_UPDATERS)]
    readers = [Reader.remote(table_holder) for _ in range(NUM_READERS)]
    sweeper = Sweeper.remote(table_holder)

    # Mock methods
    for writer in writers:
        writer.add_row = Mock()
    for updater in updaters:
        updater.update_row = Mock()
    for reader in readers:
        reader.read_rows = Mock()
    sweeper.remove_old_rows = Mock()

    # Simulate system behavior
    for writer in writers:
        ray.get(writer.add_row.remote())
    for updater in updaters:
        ray.get(updater.update_row.remote())
    for reader in readers:
        ray.get(reader.read_rows.remote(True, 10))
    ray.get(sweeper.remove_old_rows.remote())

    # Assert that methods were called
    for writer in writers:
        writer.add_row.assert_called()
    for updater in updaters:
        updater.update_row.assert_called()
    for reader in readers:
        reader.read_rows.assert_called()
    sweeper.remove_old_rows.assert_called()

    # Verify logging
    with patch('logging.info') as mock_logging_info:
        for writer in writers:
            writer.add_row()
            mock_logging_info.assert_called_with("Executing add_row")
        for updater in updaters:
            updater.update_row()
            mock_logging_info.assert_called_with("Executing update_row")
        for reader in readers:
            reader.read_rows(True, 10)
            mock_logging_info.assert_called_with("Executing read_rows with impression_is_null=True, num_rows=10")
        sweeper.remove_old_rows()
        mock_logging_info.assert_called_with("Executing remove_old_rows with expiration_minutes=60")
