import os

import json
import yaml
import unittest

import psycopg2
import psycopg2.extras
import pytest

from typing import Any, Dict, Iterable

from kafka import KafkaConsumer, TopicPartition
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from minio import Minio
from minio.error import S3Error
from testcontainers.minio import MinioContainer

from object_store_connector.connector import ObjectStoreConnector
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.utils import EncryptionUtil, Config
from obsrv.models import Metric
from obsrv.connector.batch import ISourceConnector, SourceConnector
from obsrv.job.batch import get_base_conf

from tests.batch_setup import setup_obsrv_database  # noqa


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    minio_container = MinioContainer("minio/minio:latest")
    minio_container.start()

    connector_config = json.dumps({
        "source_prefix": "",
        "source_type": "s3",
        "source_data_format": "jsonl",
        "source_bucket": "testing-bucket",
        "source_credentials_region": "us-east-2",
        "source_credentials_secret_key": minio_container.secret_key,
        "source_credentials_access_key": minio_container.access_key,
        "source_credentials_endpoint": f"{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}"
    })
    insert_connector(connector_config)
    init_minio(connector_config)

    def cleanup():
        minio_container.stop()

    request.addfinalizer(cleanup)


def init_minio(connector_config):
    config = json.loads(connector_config)
    minio_client = Minio(
        endpoint=config["source_credentials_endpoint"],
        access_key=config["source_credentials_access_key"],
        secret_key=config["source_credentials_secret_key"],
        region=config["source_credentials_region"],
        secure=False
    )

    bucket_name = config['source_bucket']

    # Create Bucket
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except S3Error as e:
        print(f"[ERROR] Error while creating MINIO bucket: {e}")

    object_name = "data.json"
    file_path = "./stubs/events/chunk-4997.json"
    c_type = "application/json"

    # Upload Objects
    try:
        minio_client.fput_object(
            bucket_name,
            object_name,
            file_path,
            c_type
        )

    except S3Error as e:
        print(f"Error uploading {object_name} : {e}")
    except FileNotFoundError:
        print(f"File not found: {object_name}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return minio_client


def insert_connector(connector_config):
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "r"
    ) as config_file:
        config = yaml.safe_load(config_file)

    enc = EncryptionUtil(config["obsrv_encryption_key"])

    ins_cr = """
        INSERT INTO connector_registry (
            id,
            connector_id,
            "name",
            "type",
            category,
            "version",
            description,
            technology,
            runtime,
            licence,
            "owner",
            iconurl,
            status,
            ui_spec,
            source_url,
            "source",
            created_by,
            updated_by,
            created_date,
            updated_date,
            live_date
        ) VALUES (
            'aws-s3-connector-0.1.0',
            'aws-s3-connector',
            'AWS S3',
            'source',
            'batch',
            '0.1.0',
            'test_reader',
            'Python',
            'Spark',
            'MIT',
            'ravi@obsrv.ai',
            'http://localhost',
            'Live',
            '{}',
            'object_store_connector-0.1.0.tar_8b84dc.gz',
            '{}',
            'SYSTEM',
            'SYSTEM',
            now(),
            now(),
            now()
        );
    """

    enc_config = enc.encrypt(connector_config)

    ins_ci = """
        INSERT INTO connector_instances (
            id,
            dataset_id,
            connector_id,
            connector_config,
            operations_config,
            status,
            connector_state,
            connector_stats,
            created_by,
            updated_by,
            created_date,
            updated_date,
            published_date
        ) VALUES (
            's3.new-york-taxi-data.1',
            'new-york-taxi-data',
            'aws-s3-connector-0.1.0',
            %s,
            '{}',
            'Live',
            '{}',
            '{}',
            'SYSTEM',
            'SYSTEM',
            now(),
            now(),
            now()
        );
    """

    conn = psycopg2.connect(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        dbname=config["postgres"]["dbname"],
    )

    cur = conn.cursor()

    cur.execute(ins_cr)
    cur.execute(ins_ci, (json.dumps(enc_config),))

    conn.commit()
    conn.close()


# class TestSource(ISourceConnector):
#     def process(
#         self,
#         sc: SparkSession,
#         ctx: ConnectorContext,
#         connector_config: Dict[Any, Any],
#         metrics_collector: MetricsCollector,
#     ) -> Iterable[DataFrame]:
#         df = sc.read.format("json").load("tests/sample_data/nyt_data_100.json.gz")
#         yield df

#         df1 = sc.read.format("json").load("tests/sample_data/nyt_data_100.json")

#         yield df1

#     def get_spark_conf(self, connector_config) -> SparkConf:
#         conf = get_base_conf()
#         return conf


# @pytest.mark.usefixtures("setup_obsrv_database")
class TestBatchConnector(unittest.TestCase):
    def test_source_connector(self):
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)
        config = Config(config_file_path)

        # These variables are currently hard coded
        test_raw_topic = "dev.ingest"
        test_metrics_topic = "s3.metrics"

        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config.find("kafka.broker-servers"),
            group_id="s3-group",
            enable_auto_commit=True,
            auto_offset_reset='earliest'
        )

        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        # kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        connector_state = None
        connector_stats = None

        postgres_config = config.find("postgres")
        conn = psycopg2.connect(
            host=postgres_config["host"],
            port=postgres_config["port"],
            user=postgres_config["user"],
            password=postgres_config["password"],
            dbname=postgres_config["dbname"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM connector_instances;")
        connector_instance = cur.fetchone()
        connector_state = connector_instance["connector_state"]
        connector_stats = connector_instance["connector_stats"]
        conn.close

        obj_count = 1
        records_per_obj = 200
        expected_records = obj_count * records_per_obj

        # Postgres asserts
        assert connector_stats["num_files_discovered"] == obj_count
        assert connector_stats["num_files_processed"] == obj_count
        assert connector_state["to_process"] == []

        metrics = []
        all_messages = kafka_consumer.poll(timeout_ms=10000)
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    metrics.append(json.loads(message.value))

        api_calls_ListObjectsV2, errors_ListObjectsV2 = 0, 0
        api_calls_getObjectTagging, errors_getObjectTagging = 0, 0
        api_calls_getObject, errors_getObject = 0, 0
        api_calls_setObjectTagging, errors_setObjectTagging = 0, 0

        for metric in metrics:
            for d in metric["edata"]["labels"]:
                if "method_name" != d["key"]:
                    continue
                if d["value"] == "ListObjectsV2":
                    api_calls_ListObjectsV2 += metric["edata"]["metric"]["num_api_calls"]
                    errors_ListObjectsV2 += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObjectTagging":
                    api_calls_getObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObject":
                    api_calls_getObject += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObject += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "setObjectTagging":
                    api_calls_setObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_setObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break

        print(f"ListObjectsV2 requests: {api_calls_ListObjectsV2}, errors: {errors_ListObjectsV2}")
        print(f"getObjectTagging requests: {api_calls_getObjectTagging}, errors: {errors_getObjectTagging}")
        print(f"getObjectTagging requests: {api_calls_getObjectTagging}, errors: {errors_getObject}")
        print(f"setObjectTagging requests: {api_calls_setObjectTagging}, errors: {errors_setObjectTagging}")

        # Kafka asserts
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_records}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: 6}

        # Objects discovered metrics
        assert metrics[obj_count + 1]["edata"]["metric"]["new_objects_discovered"] == obj_count

        # Execution metrics
        assert metrics[-1]["edata"]["metric"]["total_records_count"] == expected_records
        assert metrics[-1]["edata"]["metric"]["success_records_count"] == expected_records
        assert metrics[-1]["edata"]["metric"]["failed_records_count"] == 0
