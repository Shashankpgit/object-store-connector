import os
import sys
import json
import unittest

import yaml
import psycopg2
import psycopg2.extras
import pytest

from kafka import KafkaConsumer, TopicPartition

from obsrv.utils import EncryptionUtil, Config
from obsrv.connector.batch import SourceConnector

sys.path.insert(0, os.path.join(os.getcwd(), "object_store_connector"))

from object_store_connector.connector import ObjectStoreConnector

from tests.batch_setup import setup_obsrv_database  # noqa

# Import your gcs config here
from stubs.obsrv_encrypt import gcs_config_new


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    connector_instance_id = "gcs.new-york-taxi-data.1"

    connector_config = json.dumps(gcs_config_new)
    insert_connector(connector_config, connector_instance_id)

    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml")
    ) as config_file:
        config = yaml.safe_load(config_file)
        config["connector_instance_id"] = connector_instance_id
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "w"
    ) as config_file:
        yaml.dump(config, config_file)


def insert_connector(connector_config, connector_instance_id):
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "r"
    ) as config_file:
        config = yaml.safe_load(config_file)

    enc = EncryptionUtil(config["obsrv_encryption_key"])
    enc_config = enc.encrypt(connector_config)

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
            'gcs-connector-0.1.0',
            'gcs-connector',
            'GCS connector',
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
            %s,
            'new-york-taxi-data',
            'gcs-connector-0.1.0',
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
    cur.execute(ins_ci, (connector_instance_id, json.dumps(enc_config),))

    conn.commit()
    conn.close()


# @pytest.mark.usefixtures("setup_obsrv_database")
class TestBatchConnector(unittest.TestCase):
    def test_source_connector(self):
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)
        config = Config(config_file_path)

        test_raw_topic = "dev.ingest"  # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

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
        cur.execute(f"SELECT * FROM connector_instances WHERE id = '{config.find("connector_instance_id")}';")
        connector_instance = cur.fetchone()
        connector_state = connector_instance["connector_state"]
        connector_stats = connector_instance["connector_stats"]
        conn.close

        obj_count = 1
        records_per_obj = 200
        expected_record_count = obj_count * records_per_obj

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

        api_calls_list_blobs, errors_list_blobs = 0, 0
        api_calls_get_blob_metadata, errors_get_blob_metadata = 0, 0
        api_calls_get_blob, errors_get_blob = 0, 0
        api_calls_update_blob_metadata, errors_update_blob_metadata = 0, 0

        for metric in metrics:
            for d in metric["edata"]["labels"]:
                if "method_name" != d["key"]:
                    continue
                if d["value"] == "listBlobs":
                    api_calls_list_blobs += metric["edata"]["metric"]["num_api_calls"]
                    errors_list_blobs += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getBlobMetadata":
                    api_calls_get_blob_metadata += metric["edata"]["metric"]["num_api_calls"]
                    errors_get_blob_metadata += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getBlob":
                    api_calls_get_blob += metric["edata"]["metric"]["num_api_calls"]
                    errors_get_blob += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "updateBlobMetadata":
                    api_calls_update_blob_metadata += metric["edata"]["metric"]["num_api_calls"]
                    errors_update_blob_metadata += metric["edata"]["metric"]["num_errors"]
                    break
        
        try:
            print(f"listBlobs requests: {api_calls_list_blobs}, errors: {errors_list_blobs}")
            print(f"getBlobMetadata requests: {api_calls_get_blob_metadata}, errors: {errors_get_blob_metadata}")
            print(f"getBlob requests: {api_calls_get_blob}, errors: {errors_get_blob}")
            print(f"updateBlobMetadata requests: {api_calls_update_blob_metadata}, errors: {errors_update_blob_metadata}")
            print(f"Total exec time: {metrics[-1]["edata"]["metric"]["total_exec_time_ms"]}")
            print(f"Framework exec time: {metrics[-1]["edata"]["metric"]["fw_exec_time_ms"]}")
            print(f"Connector exec time: {metrics[-1]["edata"]["metric"]["connector_exec_time_ms"]}")
        except Exception as e:
            print(f"[ERROR] Error in printing stats: {e}")

        # Api call count asserts
        assert api_calls_get_blob_metadata   \
            == api_calls_get_blob          \
            == api_calls_update_blob_metadata   \
            == obj_count

        # Number of times metrics_collector.collect() should be called
        # SDK => 1 execution metric, connector => 1 num_objects_discovered metric, so + 2
        expected_metric_count = api_calls_list_blobs + 3 * obj_count + 2

        # Kafka asserts
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_record_count}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: expected_metric_count}

        # Objects discovered metrics
        assert metrics[
            int(api_calls_list_blobs + api_calls_get_blob_metadata)
        ]["edata"]["metric"]["new_objects_discovered"] == obj_count

        # Execution metrics
        assert metrics[-1]["edata"]["metric"]["total_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["success_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["failed_records_count"] == 0
