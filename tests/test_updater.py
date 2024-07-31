import pytest
import ray
from unittest.mock import Mock, patch
from actors.updater import Updater

@pytest.fixture
def mock_table_holder():
    return Mock()

@pytest.fixture
def updater(mock_table_holder):
    return Updater(mock_table_holder)

def test_update_row(updater, mock_table_holder):
    mock_table_holder.get_random_uuid.remote.return_value = "random-uuid"
    updater.update_row()
    mock_table_holder.conn.execute.assert_called_once_with('''
        UPDATE EVENTS
        SET IMPRESSION = ?, IMPRESSIONTIME = ?, ENGAGEMENT = ?, ENGAGEMENTTIME = ?
        WHERE ID = ?
    ''', (True, mock.ANY, None, None, "random-uuid"))
    assert updater.total_requests == 1

def test_run(updater, mock_table_holder):
    with patch.object(updater, 'update_row') as mock_update_row:
        with patch('time.sleep', return_value=None):
            updater.run(1)
            mock_update_row.assert_called_once()
            assert updater.total_requests == 1
