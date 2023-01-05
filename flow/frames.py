from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Optional, Dict, Any, Set

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from .common import Source, Transformer, Join, Stub

SchemaLike = Union[StructType, str]


@dataclass(frozen=True)
class DFFileSource(Source[SparkSession, DataFrame]):
    path: Path
    format: str
    schema: Optional[SchemaLike] = None
    options: Dict[str, Any] = field(default_factory=lambda: dict())

    def __call__(self, spark: SparkSession) -> DataFrame:
        return (
            spark.read.format(self.format)
            .options(**self.options)
            .load(str(self.path), schema=self.schema)
        )


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


@dataclass(kw_only=True, frozen=True, init=False)
class DFQuery(Join[SparkSession, DataFrame]):
    sql_query: str

    def __init__(self, names: Set[str], sql_query: str) -> None:
        super().__init__(names)
        object.__setattr__(self, "sql_query", sql_query)

    def __call__(self, ctx: SparkSession, **args: DataFrame) -> DataFrame:
        for name, df in args.items():
            df.createOrReplaceTempView(name)
        return ctx.sql(self.sql_query)


@dataclass(frozen=True)
class DFTableSink(Stub[SparkSession, DataFrame]):
    table_name: str
    format: str
    mode: str
    path: Path

    def __call__(self, ctx: SparkSession, arg: DataFrame) -> None:
        arg.write.format(self.format).mode(self.mode).option(
            "path", str(self.path)
        ).saveAsTable(self.table_name)
