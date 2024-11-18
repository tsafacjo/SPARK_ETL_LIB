
from abc import ABC, abstractmethod

class DataSource(ABC):
    """
    Abstract class
    """

    def __init__(self, spark, path):
        self.spark = spark
        self.path = path

    @abstractmethod
    def get_data_frame(self):
        """
        Abstract method,
        """
        raise ValueError("Not Implementeed")
