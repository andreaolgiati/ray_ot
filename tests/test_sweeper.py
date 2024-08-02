import pytest
import ray
from unittest.mock import Mock, patch
from actors.sweeper import Sweeper
from opentelemetry import trace

@pytest.fixture
def mock_table_holder():
    return Mock()

@pytest.fixture
def sweeper(mock_table_holder):
    return Sweeper(mock_table_holder, 60)

def test_remove_old_rows(sweeper, mock_table_holder):
    sweeper.remove_old_rows()
    mock_table_holder.conn.execute.assert_called_once_with('''
        DELETE FROM EVENTS
        WHERE CREATIONTIME < NOW() - INTERVAL ? MINUTE
    ''', (60,))
    assert sweeper.total_requests == 1

    # Verify OTEL logging
    span = trace.get_current_span()
    assert span.name == "remove_old_rows"

    # Verify logging
    with patch('logging.info') as mock_logging_info:
        sweeper.remove_old_rows()
        mock_logging_info.assert_called_with("Executing remove_old_rows with expiration_minutes=60")

def test_run(sweeper, mock_table_holder):
    with patch.object(sweeper, 'remove_old_rows') as mock_remove_old_rows:
        with patch('time.sleep', return_value=None):
            sweeper.run()
            mock_remove_old_rows.assert_called_once()
            assert sweeper.total_requests == 1

def test_print_stats(sweeper):
    with patch('logging.info') as mock_logging_info:
        sweeper.print_stats()
        mock_logging_info.assert_called()
