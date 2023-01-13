"""Module providing unit tests for Pythoid Spark Streaming blocks."""
import logging
import shutil
import tempfile
import unittest
import warnings
from pathlib import Path
from time import sleep

from pyspark import Row
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.streaming import StreamingQuery

from flow.frames import DFFilter, DFSingleTableQuery
from flow.streams import StreamFileSink, StreamFileSource
from tests import LOG_DATE_FORMAT, LOG_MSG_FORMAT, data_filepath


class StreamTestCase(unittest.TestCase):
    """Test suite for Pythoid Spark Streaming blocks."""

    spark: SparkSession

    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        cls.spark = SparkSession.builder.getOrCreate()

    def setUp(self) -> None:
        self.testdir = Path(tempfile.mkdtemp())
        self.srcdir = self.testdir / "src"
        self.srcdir.mkdir()
        self.tgtdir = self.testdir / "tgt"
        self.chkdir = self.testdir / "chk"
        self.dbdir = self.testdir / "db"
        self.dbname = self.testdir.name
        self.spark.sql(f"CREATE DATABASE {self.dbname} LOCATION '{self.dbdir}'")

    def tearDown(self) -> None:
        self.spark.sql(f"DROP DATABASE {self.dbname} CASCADE")
        shutil.rmtree(self.testdir)

    @classmethod
    def tearDownClass(cls) -> None:
        for stream in cls.spark.streams.active:
            if stream:
                stream.processAllAvailable()
                stream.stop()
        cls.spark.stop()

    def test_filesource(self):
        """Tests StreamFileSource block."""
        schema = "name string, sex string, age int"
        src = StreamFileSource(
            path=self.srcdir, format="csv", schema=schema, options={"header": True}
        )
        tgt = src(self.spark)

        self._run_simulation(tgt, "append")
        df = self.spark.sql(f"select * from {self.dbname}.result")
        self.assertSetEqual(
            set(df.collect()),
            {
                Row(name="john", sex="M", age=25, batch=0),
                Row(name="jane", sex="F", age=34, batch=0),
                Row(name="jack", sex="M", age=17, batch=1),
                Row(name="josh", sex="M", age=52, batch=1),
                Row(name="jill", sex="F", age=44, batch=1),
                Row(name="jake", sex="M", age=39, batch=2),
            },
        )

    def test_pipeline(self):
        """Tests simple Spark Streaming pipeline."""
        schema = "name string, sex string, age int"
        src = StreamFileSource(
            path=self.srcdir, format="csv", schema=schema, options={"header": True}
        )
        eligible = DFFilter("age >= 30")
        counts = DFSingleTableQuery(
            "select sex, count(*) as count from people group by sex", "people"
        )
        pipeline = src >> eligible >> counts
        tgt = pipeline(self.spark)

        self._run_simulation(tgt, "complete")
        df = self.spark.sql(f"select * from {self.dbname}.result")
        self.assertSetEqual(
            set(df.collect()),
            {
                Row(sex="F", count=1, batch=0),
                Row(sex="M", count=1, batch=1),
                Row(sex="F", count=2, batch=1),
                Row(sex="M", count=2, batch=2),
                Row(sex="F", count=2, batch=2),
            },
        )

    def test_sink(self):
        """Tests StreamFileSink block."""
        schema = "name string, sex string, age int"
        src = StreamFileSource(
            path=self.srcdir, format="csv", schema=schema, options={"header": True}
        )
        flt = DFFilter("name like 'ja%'")
        tgt = StreamFileSink(path=self.tgtdir, format="json")
        pipeline = src >> flt > tgt
        sq: StreamingQuery = pipeline(self.spark)
        self._run_people_stream()
        sq.stop()
        df = self.spark.read.json(str(self.tgtdir))
        self.assertSetEqual(
            set(df.collect()),
            {
                Row(age=34, name="jane", sex="F"),
                Row(age=17, name="jack", sex="M"),
                Row(age=39, name="jake", sex="M"),
            },
        )

    def _run_simulation(self, tgt: DataFrame, out_mode: str):
        def test_batch(df: DataFrame, batch_id: int):
            df.withColumn("batch", lit(batch_id)).write.saveAsTable(
                f"{self.dbname}.result", mode="append"
            )

        query = (
            tgt.writeStream.trigger(processingTime="400 millisecond")
            .outputMode(out_mode)
            .option("checkpointLocation", str(self.chkdir))
            .foreachBatch(test_batch)
            .start()
        )

        self._run_people_stream()

        query.stop()

    def _run_people_stream(self):
        for i in range(1, 4):
            shutil.copy(data_filepath(f"stream/people{i}.csv"), self.srcdir)
            sleep(3)
