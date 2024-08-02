import pytest
import ray
from unittest.mock import MagicMock, patch
from actors.writer import Writer
from opentelemetry import trace

@pytest.fixture
def table_holder():
    return MagicMock()

@pytest.fixture
def writer(table_holder):
    return Writer(table_holder)

def test_add_row(writer, table_holder):
    table_holder.add_row.remote.return_value = "test_uuid"
    with patch('logging.info') as mock_logging_info:
        writer.add_row()
        table_holder.add_row.remote.assert_called_once()
        table_holder.uuids.add.remote.assert_called_once_with("test_uuid")

        # Verify OTEL logging
        span = trace.get_current_span()
        assert span.name == "add_row"

        # Verify logging
        mock_logging_info.assert_called_with("Executing add_row")

def test_run(writer, table_holder):
    table_holder.add_row.remote.return_value = "test_uuid"
    writer.add_row = MagicMock()
    with patch('logging.info') as mock_logging_info:
        writer.run(1)
        writer.add_row.assert_called_once()
        mock_logging_info.assert_called()
