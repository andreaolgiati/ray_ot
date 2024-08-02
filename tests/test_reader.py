import pytest
import ray
from unittest.mock import Mock, patch
from actors.reader import Reader
from opentelemetry import trace

@pytest.fixture
def mock_table_holder():
    return Mock()

@pytest.fixture
def reader(mock_table_holder):
    return Reader(mock_table_holder)

def test_read_rows(reader, mock_table_holder):
    mock_table_holder.conn.execute.return_value.fetchall.return_value = [("row1",), ("row2",)]
    rows = reader.read_rows(True, 2)
    assert rows == [("row1",), ("row2",)]
    assert reader.total_requests == 1

    # Verify OTEL logging
    span = trace.get_current_span()
    assert span.name == "read_rows"

    # Verify logging
    with patch('logging.info') as mock_logging_info:
        reader.read_rows(True, 2)
        mock_logging_info.assert_called_with("Executing read_rows with impression_is_null=True, num_rows=2")

def test_run(reader, mock_table_holder):
    with patch.object(reader, 'read_rows', return_value=[("row1",), ("row2",)]) as mock_read_rows:
        with patch('time.sleep', return_value=None):
            reader.run(1, True, 2)
            mock_read_rows.assert_called_with(True, 2)
            assert reader.total_requests == 1

def test_print_stats(reader):
    with patch('logging.info') as mock_logging_info:
        reader.print_stats()
        mock_logging_info.assert_called()
