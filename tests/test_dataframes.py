import unittest

from pyspark.sql import SparkSession

from flow.dataframes import DFFileSource, DFFilter, DFSingleTableQuery, DFQuery, DFTableSink


class DFramesTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.getOrCreate()

    def test_filesource(self):
        schema = "name string, sex string, age int"
        src = DFFileSource(path="data/people.csv", format="csv", schema=schema, options={"header": True})
        df = src(self.spark)
        df.show()

    def test_pipeline(self):
        schema = "name string, sex string, age int"
        src = DFFileSource(path="data/people.csv", format="csv", schema=schema, options={"header": True})
        eligible = DFFilter("age >= 18")
        counts = DFSingleTableQuery("select sex, count(*) as count from people group by sex", "people")
        pipeline = src >> eligible >> counts
        df = pipeline(self.spark)
        df.show()

    def test_joined_pipeline(self):
        people = DFFileSource(path="data/people.csv",
                              format="csv",
                              schema="name string, sex string, age int",
                              options={"header": True})
        scores = DFFileSource(path="data/scores.csv",
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
        # pipeline = scores >= (people >= (join, "people"), "scores")
        pipeline = scores.to_join(people.to_join(join, "people"), "scores")
        df = pipeline(self.spark)
        df.show()
        sink = DFTableSink("result", "parquet", "overwrite", "data/result.parquet")
        pipe2 = pipeline > sink
        pipe2(self.spark)
