from dataclasses import dataclass
from typing import Union, Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from .common import Source

SchemaLike = Union[StructType, str]


@dataclass(frozen=True)
class StreamFileSource(Source[SparkSession, DataFrame]):
    path: str
    format: str
    schema: Optional[SchemaLike] = None
    options: Dict[str, Any] = None

    def __call__(self, spark: SparkSession) -> DataFrame:
        return spark.readStream.format(self.format).options(**self.options).load(self.path, schema=self.schema)
