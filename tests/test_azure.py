import os
import unittest
from typing import Any, Dict
import sys
sys.path.insert(0, '/home/vince/repogit/object-store-connector/object_store_connector')

import pytest
import yaml
from kafka import KafkaConsumer, TopicPartition
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession

from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.connector.batch import ISourceConnector, SourceConnector
from obsrv.job.batch import get_base_conf
from tests.batch_setup import setup_obsrv_database  # noqa

from object_store_connector.connector import ObjectStoreConnector

import json


class TestSource(ISourceConnector):
    def process(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector,
    ) -> DataFrame:
        df = sc.read.format("json").load("tests/sample_data/nyt_data_100.json.gz")
    
        yield df

        df1 = sc.read.format("json").load("tests/sample_data/nyt_data_100.json")

        yield df1

    def get_spark_conf(self, connector_config) -> SparkConf:
        conf = get_base_conf()
        return conf


@pytest.mark.usefixtures("setup_obsrv_database")
class TestBatchConnector(unittest.TestCase):

    def test_source_connector(self):

        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        config = yaml.safe_load(open(config_file_path))


        self.assertEqual(os.path.exists(config_file_path), True)

        test_raw_topic = "azure.ingest"
        test_metrics_topic = "azure.metrics"
        
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config["kafka"]["broker-servers"],
            group_id="azure-test-group",
            enable_auto_commit=True,
        )
        
        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        
        all_messages = kafka_consumer.poll(timeout_ms=10000)

        metrics = []
        metric_names={
                'num_api_calls': 0, 
                'new_objects_discovered': 0,
                'total_records_count':0,
                'num_records':0,
                'failed_records_count':0
            }
        
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    msg_val=json.loads(message.value.decode())
                    metrics.append(msg_val)
                    
        for metric in metrics:
            metric_data = metric['edata']['metric']  
            for mn, val in metric_names.items():  
                if mn in metric_data:  
                    metric_names[mn] += metric_data[mn]
      
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: 200}
       
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: 9}
        
        
        assert metric_names['num_api_calls'] == 7
        assert metric_names['new_objects_discovered'] == 2
        assert metric_names['total_records_count'] == 200
        assert metric_names['num_records'] == 200
        assert metric_names['failed_records_count'] == 0