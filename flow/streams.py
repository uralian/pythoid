"""Module providing building blocks for Pythoid Spark Streaming dataflows."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from .common import Source, Stub

SchemaLike = Union[StructType, str]


@dataclass(frozen=True)
class StreamFileSource(Source[SparkSession, DataFrame]):
    """Loads data from an external file and produces a stream of dataframes."""

    path: Path
    format: str
    schema: Optional[SchemaLike] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        super().__init__()

    def _do_call__(self, ctx: SparkSession) -> DataFrame:
        reader = ctx.readStream.format(self.format).options(**self.options)
        return reader.load(str(self.path), schema=self.schema)


@dataclass(frozen=True)
class StreamFileSink(Stub[SparkSession, DataFrame]):
    """Loads data stream to an external file."""

    path: Path
    format: str

    def __post_init__(self):
        super().__init__()

    def _do_call__(self, ctx: SparkSession, arg: DataFrame) -> StreamingQuery:
        return (
            arg.writeStream.trigger(processingTime="500 millisecond")
            .format("json")
            .outputMode("append")
            .option("checkpointLocation", str(self.path / "cp"))
            .start(path=str(self.path))
        )
