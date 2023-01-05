from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from .common import Source, Stub

SchemaLike = Union[StructType, str]


@dataclass(frozen=True)
class StreamFileSource(Source[SparkSession, DataFrame]):
    path: Path
    format: str
    schema: Optional[SchemaLike] = None
    options: Dict[str, Any] = field(default_factory=lambda: dict())

    def __call__(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format(self.format).options(**self.options)
        return reader.load(str(self.path), schema=self.schema)


@dataclass(frozen=True)
class StreamFileSink(Stub[SparkSession, DataFrame]):
    path: Path
    format: str

    def __call__(self, spark: SparkSession, arg: DataFrame) -> StreamingQuery:
        return arg.writeStream \
            .trigger(processingTime="500 millisecond") \
            .format("json") \
            .outputMode("append") \
            .option("checkpointLocation", str(self.path / "cp")) \
            .start(path=str(self.path))
