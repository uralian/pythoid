import unittest

from pyspark.sql import SparkSession

from flow.dataframes import DFFileSource, DFFilter, DFSingleTableQuery, DFQuery, DFTableSink
from flow.streams import StreamFileSource


class StreamTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.getOrCreate()

    def test_filesource(self):
        schema = "name string, sex string, age int"
        src = StreamFileSource(path="data/people.csv", format="csv", schema=schema, options={"header": True})
        df = src(self.spark)
        df.writeStream.start()
