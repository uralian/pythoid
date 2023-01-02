from dataclasses import dataclass
from typing import Union, Optional, Dict, Any, Set

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from .common import Source, Transformer, Join, SinkT

SchemaLike = Union[StructType, str]


@dataclass(frozen=True)
class DFFileSource(Source[SparkSession, DataFrame]):
    path: str
    format: str
    schema: Optional[SchemaLike] = None
    options: Dict[str, Any] = None

    def __call__(self, spark: SparkSession) -> DataFrame:
        return spark.read.format(self.format).options(**self.options).load(self.path, schema=self.schema)


@dataclass(frozen=True)
class DFFilter(Transformer[SparkSession, DataFrame]):
    condition: str

    def __call__(self, spark: SparkSession, arg: DataFrame) -> DataFrame:
        return arg.filter(self.condition)


@dataclass(frozen=True)
class DFSingleTableQuery(Transformer[SparkSession, DataFrame]):
    sql_query: str
    alias: str

    def __call__(self, spark: SparkSession, arg: DataFrame) -> DataFrame:
        arg.createOrReplaceTempView(self.alias)
        return spark.sql(self.sql_query)


@dataclass(frozen=True)
class DFQuery(Join[SparkSession, DataFrame]):
    names: Set[str]
    sql_query: str

    def input_names(self) -> Set[str]:
        return self.names

    def __call__(self, spark: SparkSession, **args: DataFrame) -> DataFrame:
        for name, df in args.items():
            df.createOrReplaceTempView(name)
        return spark.sql(self.sql_query)


@dataclass(frozen=True)
class DFTableSink(SinkT[SparkSession, DataFrame]):
    table_name: str
    format: str
    mode: str
    path: str

    def __call__(self, ctx: SparkSession, arg: DataFrame) -> None:
        arg.write.format(self.format).mode(self.mode).option("path", self.path).saveAsTable(self.table_name)
