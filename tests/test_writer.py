import pytest
import ray
from unittest.mock import MagicMock
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
    writer.add_row()
    table_holder.add_row.remote.assert_called_once()
    table_holder.uuids.add.remote.assert_called_once_with("test_uuid")

    # Verify OTEL logging
    span = trace.get_current_span()
    assert span.name == "add_row"

def test_run(writer, table_holder):
    table_holder.add_row.remote.return_value = "test_uuid"
    writer.add_row = MagicMock()
    writer.run(1)
    writer.add_row.assert_called_once()
