import tempfile
import unittest
import warnings

from pyspark import Row
from pyspark.sql import SparkSession

from flow.frames import DFFileSource, DFFilter, DFSingleTableQuery, DFQuery, DFTableSink
from tests import data_filepath


class DFramesTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        cls.spark = SparkSession.builder.getOrCreate()
        cls.people_file = data_filepath("people.csv")
        cls.scores_file = data_filepath("scores.csv")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_filesource(self):
        schema = "name string, sex string, age int"
        src = DFFileSource(path=self.people_file, format="csv", schema=schema, options={"header": True})
        df = src(self.spark)
        self.assertSetEqual(set(df.collect()), {
            Row(name="john", sex="M", age=25),
            Row(name="jane", sex="F", age=34),
            Row(name="jack", sex="M", age=17),
            Row(name="josh", sex="M", age=52),
            Row(name="jill", sex="F", age=44),
            Row(name="jake", sex="M", age=39)
        })

    def test_pipeline(self):
        schema = "name string, sex string, age int"
        src = DFFileSource(path=self.people_file, format="csv", schema=schema, options={"header": True})
        eligible = DFFilter("age >= 18")
        counts = DFSingleTableQuery("select sex, count(*) as count from people group by sex", "people")
        pipeline = src >> eligible >> counts
        df = pipeline(self.spark)
        self.assertSetEqual(set(df.collect()), {
            Row(sex="M", count=3),
            Row(sex="F", count=2)
        })

    def test_joined_pipeline(self):
        people = DFFileSource(path=self.people_file,
                              format="csv",
                              schema="name string, sex string, age int",
                              options={"header": True})
        scores = DFFileSource(path=self.scores_file,
                              format="csv",
                              schema="person string, subject string, score int",
                              options={"header": True})
        join = DFQuery(
            {"people", "scores"},
            """
            select people.name, people.age, scores.subject, scores.score
            from people inner join scores on people.name=scores.person
            """
        )
        pipeline = scores >= (people >= (join, "people"), "scores")
        df = pipeline(self.spark)
        self.assertSetEqual(set(df.collect()), {
            Row(name="jake", age=39, subject="math", score=93),
            Row(name="jane", age=34, subject="math", score=87),
            Row(name="jake", age=39, subject="history", score=91),
            Row(name="jack", age=17, subject="arts", score=78),
            Row(name="jane", age=34, subject="arts", score=99),
            Row(name="john", age=25, subject="math", score=85),
            Row(name="josh", age=52, subject="history", score=75),
        })

    def test_sink(self):
        src = DFFileSource(path=self.people_file,
                           format="csv",
                           schema="name string, sex string, age int",
                           options={"header": True})
        tgt_path = tempfile.mkdtemp()
        sink = DFTableSink("result", "parquet", "overwrite", tgt_path)
        pipeline = src > sink
        pipeline(self.spark)
        df = self.spark.read.parquet(tgt_path)
        self.assertSetEqual(set(df.collect()), {
            Row(name="john", sex="M", age=25),
            Row(name="jane", sex="F", age=34),
            Row(name="jack", sex="M", age=17),
            Row(name="josh", sex="M", age=52),
            Row(name="jill", sex="F", age=44),
            Row(name="jake", sex="M", age=39)
        })
