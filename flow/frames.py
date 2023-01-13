"""Module providing building blocks for Pythoid Spark dataflows."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .common import Join, Source, Stub, Transformer

SchemaLike = Union[StructType, str]


@dataclass(frozen=True)
class DFFileSource(Source[SparkSession, DataFrame]):
    """Loads data from an external file and produces a dataframe."""

    path: Path
    format: str
    schema: Optional[SchemaLike] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        super().__init__()

    def _do_call__(self, ctx: SparkSession) -> DataFrame:
        return (
            ctx.read.format(self.format)
            .options(**self.options)
            .load(str(self.path), schema=self.schema)
        )


@dataclass(frozen=True)
class DFFilter(Transformer[SparkSession, DataFrame]):
    """Filters a dataframe using a condition."""

    condition: str

    def __post_init__(self):
        super().__init__()

    def _do_call__(self, ctx: SparkSession, arg: DataFrame) -> DataFrame:
        return arg.filter(self.condition)


@dataclass(frozen=True)
class DFSingleTableQuery(Transformer[SparkSession, DataFrame]):
    """Runs an SQL query on a single dataframe."""

    sql_query: str
    alias: str

    def __post_init__(self):
        super().__init__()

    def _do_call__(self, ctx: SparkSession, arg: DataFrame) -> DataFrame:
        arg.createOrReplaceTempView(self.alias)
        return ctx.sql(self.sql_query)


@dataclass(kw_only=True, frozen=True, init=False)
class DFQuery(Join[SparkSession, DataFrame]):
    """Runs an SQL query on multiple dataframes."""

    sql_query: str

    def __init__(self, names: Set[str], sql_query: str) -> None:
        super().__init__(names)
        object.__setattr__(self, "sql_query", sql_query)

    def _do_call__(self, ctx: SparkSession, **args: DataFrame) -> DataFrame:
        for name, df in args.items():
            df.createOrReplaceTempView(name)
        return ctx.sql(self.sql_query)


@dataclass(frozen=True)
class DFTableSink(Stub[SparkSession, DataFrame]):
    """Saves a dataframe to a table."""

    table_name: str
    format: str
    mode: str
    path: Path

    def __post_init__(self):
        super().__init__()

    def _do_call__(self, ctx: SparkSession, arg: DataFrame) -> None:
        arg.write.format(self.format).mode(self.mode).option(
            "path", str(self.path)
        ).saveAsTable(self.table_name)
