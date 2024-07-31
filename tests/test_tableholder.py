import pytest
import ray
from actors.tableholder import TableHolder

@pytest.fixture
def table_holder():
    return TableHolder()

def test_add_row(table_holder):
    features = [0.1, 0.2, 0.3]
    result = 0.5
    row_id = table_holder.add_row(features, result)
    assert row_id in table_holder.uuids

def test_get_random_uuid(table_holder):
    features = [0.1, 0.2, 0.3]
    result = 0.5
    table_holder.add_row(features, result)
    random_uuid = table_holder.get_random_uuid()
    assert random_uuid in table_holder.uuids

def test_get_table(table_holder):
    table = table_holder.get_table()
    assert table is not None
    assert table.columns == ['ID', 'CREATIONTIME', 'FEATURES', 'RESULT', 'IMPRESSION', 'IMPRESSIONTIME', 'ENGAGEMENT', 'ENGAGEMENTTIME']
