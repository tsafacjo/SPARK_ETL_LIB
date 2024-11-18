from abc import ABC

import pytest

from etl_spark.core.datasource import DataSource

class TestDataSource:
    def test_should_be_possible_to_instanciate_datasource(self):
        assert issubclass(DataSource, ABC)  # Check if the class inherits from ABC
        with pytest.raises(TypeError):
            DataSource(None)