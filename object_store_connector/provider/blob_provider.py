from abc import ABC, abstractmethod
from typing import Any, Dict, List, final

from obsrv.connector import MetricsCollector
from pyspark.sql import DataFrame, SparkSession
from obsrv.common import ObsrvException
from obsrv.models import ErrorData

from models.object_info import ObjectInfo, Tag


class JobConfig:
    pass


class BlobProvider(ABC):
    @abstractmethod
    def get_spark_config(self, config: JobConfig) -> Dict[str, Any]:
        pass

    @abstractmethod
    def fetch_tags(
        self,
        objectPath: str,
        metrics_collector: MetricsCollector,
        config: Dict[str, Any] = None,
    ) -> List[Tag]:
        pass

    @abstractmethod
    def update_tag(
        self,
        object: ObjectInfo,
        tags: List[Tag],
        metrics_collector: MetricsCollector,
        config: Dict[str, Any] = None,
    ) -> bool:
        pass

    @abstractmethod
    def fetch_objects(
        self,
        metrics_collector: MetricsCollector,
        connectorConfig: Dict[str, Any] = None,
    ) -> List[ObjectInfo]:
        pass

    @abstractmethod
    def read_object(
        self,
        objectPath: str,
        metrics_collector: MetricsCollector,
        file_format: str,
        config: Dict[str, Any] = None,
        sc: SparkSession = None,
    ) -> DataFrame:
        pass

    @final
    def read_file(
        self,
        objectPath: str,
        metrics_collector: MetricsCollector,
        file_format: str,
        config: Dict[str, Any] = None,
        sc: SparkSession = None,
    ) -> DataFrame:
        if file_format == "jsonl":
            df = sc.read.format("json").load(objectPath)
        elif file_format == "json":
            df = sc.read.format("json").option("multiLine", True).load(objectPath)
        elif file_format == "csv":
            df = sc.read.format("csv").option("header", True).load(objectPath)
        elif file_format == "parquet":
            df = sc.read.format("parquet").load(objectPath)
        else:
            raise ObsrvException(
                ErrorData(
                    "UNSUPPORTED_FILE_FORMAT",
                    f"unsupported file format: {file_format}",
                )
            )
        return df
