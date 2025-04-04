import os

import pytest
import yaml
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

import psycopg2

# from tests.create_tables import create_tables


def create_tables(config):

    datasets = """
        CREATE TABLE public.datasets (
            id text NOT NULL,
            dataset_id text NULL,
            "type" text NOT NULL,
            "name" text NULL,
            validation_config json NULL,
            extraction_config json NULL,
            dedup_config json NULL,
            data_schema json NULL,
            denorm_config json NULL,
            router_config json NULL,
            dataset_config json NULL,
            tags _text NULL,
            data_version int4 NULL,
            status text NULL,
            created_by text NULL,
            updated_by text NULL,
            created_date timestamp DEFAULT now() NOT NULL,
            updated_date timestamp NOT NULL,
            published_date timestamp DEFAULT now() NOT NULL,
            api_version varchar(255) DEFAULT 'v1'::character varying NOT NULL,
            "version" int4 DEFAULT 1 NOT NULL,
            sample_data json DEFAULT '{}'::json NULL,
            entry_topic text DEFAULT 'dev.ingest'::text NOT NULL,
            CONSTRAINT datasets_pkey PRIMARY KEY (id)
        );
    """

    connector_registry = """
        CREATE TABLE public.connector_registry (
            id text NOT NULL,
            connector_id text NOT NULL,
            "name" text NOT NULL,
            "type" text NOT NULL,
            category text NOT NULL,
            "version" text NOT NULL,
            description text NULL,
            technology text NOT NULL,
            runtime text NOT NULL,
            licence text NOT NULL,
            "owner" text NOT NULL,
            iconurl text NULL,
            status text NOT NULL,
            ui_spec json DEFAULT '{}'::json NOT NULL,
            source_url text NOT NULL,
            "source" json NOT NULL,
            created_by text NOT NULL,
            updated_by text NOT NULL,
            created_date timestamp NOT NULL,
            updated_date timestamp NOT NULL,
            live_date timestamp NULL,
            CONSTRAINT connector_registry_connector_id_version_key UNIQUE (connector_id, version),
            CONSTRAINT connector_registry_pkey PRIMARY KEY (id)
        );
    """

    connector_instances = """
        CREATE TABLE public.connector_instances (
            id text NOT NULL,
            dataset_id text NOT NULL,
            connector_id text NOT NULL,
            connector_config text NOT NULL,
            operations_config json NOT NULL,
            status text NOT NULL,
            connector_state json DEFAULT '{}'::json NOT NULL,
            connector_stats json DEFAULT '{}'::json NOT NULL,
            created_by text NOT NULL,
            updated_by text NOT NULL,
            created_date timestamp NOT NULL,
            updated_date timestamp NOT NULL,
            published_date timestamp NOT NULL,
            "name" text NULL,
            CONSTRAINT connector_instances_pkey PRIMARY KEY (id),
            CONSTRAINT connector_instances_connector_id_fkey FOREIGN KEY (connector_id) REFERENCES public.connector_registry(id),
            CONSTRAINT connector_instances_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES public.datasets(id)
        );
    """

    ins_ds = """
        INSERT INTO datasets (
            id,
            dataset_id,
            "type",
            "name",
            validation_config,
            extraction_config,
            dedup_config,
            data_schema,
            denorm_config,
            router_config,
            dataset_config,
            tags,
            data_version,
            status,
            created_by,
            updated_by,
            created_date,
            updated_date,
            published_date,
            api_version,
            sample_data)
        VALUES(
            'new-york-taxi-data',
            'new-york-taxi-data',
            'event',
            'New York Taxi Data',
            '{"validate": true, "mode": "Strict", "validation_mode": "Strict"}',
            '{"is_batch_event": true}',
            '{"drop_duplicates": true, "dedup_key": "tripID", "dedup_period": 604800}',
            '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"tripID":{"type":"string","suggestions":[{"message":"The Property tripID appears to be uuid format type.","advice":"Suggest to not to index the high cardinal columns","resolutionType":"DEDUP","severity":"LOW","path":"properties.tripID"}],"arrival_format":"text","data_type":"string"}},"additionalProperties":false}',
            '{}',
            '{"topic": "new-york-taxi-data"}',
            '{"data_key": "", "timestamp_key": "tpep_pickup_datetime", "exclude_fields": [], "entry_topic": "azure.ingest", "redis_db_host": "obsrv-dedup-redis-master.redis.svc.cluster.local", "redis_db_port": 6379, "index_data": true, "redis_db": 0}',
            '{}',
            '2',
            'Live',
            'SYSTEM',
            'SYSTEM',
            now(),
            now(),
            now(),
            'v2',
            '{"mergedEvent": {"tripID": "4c77e9d5-538d-4eb7-8db1-4c2c32860aa8", "VendorID": "2", "tpep_pickup_datetime": "2024-09-23 00:18:42", "tpep_dropoff_datetime": "2024-09-23 00:24:38", "passenger_count": "1", "trip_distance": "1.60", "RatecodeID": "1", "store_and_fwd_flag": "N", "PULocationID": "236", "DOLocationID": "239", "payment_type": "2", "primary_passenger": {"email": "Willa67@gmail.com", "mobile": "1-720-981-6399 x77055"}, "fare_details": {"fare_amount": 7, "extra": 0.5, "mta_tax": 0.5, "tip_amount": 0, "tolls_amount": 0, "improvement_surcharge": 0.3, "total_amount": 8.3, "congestion_surcharge": 0}}}');
    """

    conn = psycopg2.connect(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        dbname=config["postgres"]["dbname"],
    )

    cur = conn.cursor()

    cur.execute(datasets)
    cur.execute(connector_registry)
    cur.execute(connector_instances)

    cur.execute(ins_ds)

    conn.commit()
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def setup_obsrv_database(request):
    postgres = PostgresContainer("postgres:latest")
    kafka = KafkaContainer("confluentinc/cp-kafka:latest")

    postgres.start()
    kafka.start()

    with open(
        os.path.join(os.path.dirname(__file__), "config/config.template.yaml")
    ) as config_file:
        config = yaml.safe_load(config_file)

        config["connector-instance-id"] = "s3.new-york-taxi-data.1"
        # config["connector-instance-id"] = "azure.new-york-taxi-data.1"

        config["postgres"]["host"] = postgres.get_container_host_ip()
        config["postgres"]["port"] = postgres.get_exposed_port(5432)
        config["postgres"]["user"] = postgres.POSTGRES_USER
        config["postgres"]["password"] = postgres.POSTGRES_PASSWORD
        config["postgres"]["dbname"] = postgres.POSTGRES_DB
        config["kafka"]["broker-servers"] = kafka.get_bootstrap_server()

    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "w"
    ) as config_file:
        yaml.dump(config, config_file)

    create_tables(config)

    # clean up
    def remove_container():
        postgres.stop()
        kafka.stop()
        try:
            os.remove(os.path.join(os.path.dirname(__file__), "config/config.yaml"))
        except FileNotFoundError:
            print("config file already removed")

    request.addfinalizer(remove_container)
