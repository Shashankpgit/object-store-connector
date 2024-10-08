import json
import os
from testcontainers.minio import MinioContainer
from minio import Minio
from minio.error import S3Error

import psycopg2
import yaml

from obsrv.utils import EncryptionUtil
from testcontainers.azurite import AzuriteContainer
from azure.storage.blob import *

def init_minio(connector_config):

    config=json.loads(connector_config)
    minio_conf = Minio(
        endpoint=config['source']['credentials']['endpoint'],
        access_key=config['source']['credentials']['access_key'],
        secret_key=config['source']['credentials']['secret_key'],
        region=config['source']['credentials']['region'],
        secure=False

    )
   
    bucket_name = config['source']['bucket']

    #Create Bucket
    try:
        if not minio_conf.bucket_exists(bucket_name):
            minio_conf.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error: {e}")

    # folder_name = "tests/sample_data"
    # file_path = os.path.join(os.getcwd(), folder_name)
    # file_path="/home/vince/repogit/object-store-connector/tests/sample_data"

    # for filename in os.listdir(file_path):    
    #     filepath=os.path.join(file_path,filename)
    object_name="data.json"
    file_path = "/home/vince/repogit/object-store-connector/tests/sample_data/nyt_data_100.json"
    c_type="application/json"
 
    #Upload Objects
    try:
        minio_conf.fput_object(
                bucket_name,
                object_name,
                file_path,
                c_type
            )
        print(f"Uploaded {object_name} in the bucket.")
    except S3Error as e:
        print(f"Error uploading {object_name} : {e}")
    except FileNotFoundError:
        print(f"File not found: {object_name}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return minio_conf



def init_azurite():
    
    azurite= AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:latest").with_bind_ports(10000,10000)
    azurite.start()

    blob_port= azurite.get_exposed_port(10000)
    conn_str=azurite.get_connection_string()
    acc_name=azurite.account_name
    acc_key=azurite.account_key

    azure_conf = {
        "accountName":acc_name,
        "accountKey":acc_key,
        "blobEndpoint":f"http://{azurite.get_container_host_ip()}:{blob_port}/{acc_name}"
    }

    blob_service_client=BlobServiceClient.from_connection_string(conn_str)
    
    
    
    container_name="test-container"

    blob_service_client.create_container(container_name)
     
    # folder_name = "tests/sample_data"
    # folder_path = os.path.join(os.getcwd(), folder_name)
    folder_path="/home/vince/repogit/object-store-connector/tests/sample_data"
    for filename in os.listdir(folder_path):    
        filepath=os.path.join(folder_path,filename)
        blob_client = blob_service_client.get_blob_client(container_name,blob=filename)
        with open(filepath, "rb") as data:
            blob_client.upload_blob(data)
    return azure_conf




def create_tables(config):
    enc = EncryptionUtil(config["obsrv_encryption_key"])
    minio_container= MinioContainer("minio/minio:latest")
    minio_container.start()
    
    connector_config = json.dumps({
            "source": {
                "type": "s3",
                "bucket": "bucket-new",
                "prefix": "/",
                "prefix_format": "v1/",
                "credentials": {
                    "access_key": minio_container.access_key,
                    "secret_key": minio_container.secret_key,
                    "endpoint": f"{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}",
                    "region":"us-east-1"
                }
            }
        })

    # Initialize Minio client
    minio_conf = init_minio(connector_config)


    azure_conf = init_azurite()
   
    datasets = """
        CREATE TABLE IF NOT EXISTS datasets (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            type TEXT NOT NULL,
            name TEXT,
            validation_config JSON,
            extraction_config JSON,
            dedup_config JSON,
            data_schema JSON,
            denorm_config JSON,
            router_config JSON,
            dataset_config JSON,
            status TEXT,
            tags TEXT[],
            data_version INT,
            created_by TEXT,
            updated_by TEXT,
            created_date TIMESTAMP NOT NULL DEFAULT now(),
            updated_date TIMESTAMP NOT NULL DEFAULT now(),
            published_date TIMESTAMP NOT NULL DEFAULT now()
        );"""

    connector_registry = """
        CREATE TABLE IF NOT EXISTS connector_registry (
            id TEXT PRIMARY KEY,
            version TEXT NOT NULL,
            type TEXT NOT NULL,
            category TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            technology TEXT NOT NULL,
            licence TEXT NOT NULL,
            owner TEXT NOT NULL,
            iconURL TEXT,
            status TEXT NOT NULL,
            created_by text NOT NULL,
            updated_by text NOT NULL,
            created_date TIMESTAMP NOT NULL DEFAULT now(),
            updated_date TIMESTAMP NOT NULL,
            live_date TIMESTAMP NOT NULL DEFAULT now()
        );"""

    connector_instances = """
        CREATE TABLE IF NOT EXISTS connector_instances (
            id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL REFERENCES datasets (id),
            connector_id TEXT NOT NULL REFERENCES connector_registry (id),
            data_format TEXT NOT NULL DEFAULT 'jsonl',
            connector_type TEXT NOT NULL,
            connector_config json NOT NULL,
            operations_config json NOT NULL,
            status TEXT NOT NULL,
            connector_state JSON,
            connector_stats JSON,
            created_by text NOT NULL,
            updated_by text NOT NULL,
            created_date TIMESTAMP NOT NULL DEFAULT now(),
            updated_date TIMESTAMP NOT NULL,
            published_date TIMESTAMP NOT NULL DEFAULT now()
        );"""

    indexes = """
        CREATE INDEX IF NOT EXISTS connector_registry_category ON connector_registry(category);
        CREATE INDEX IF NOT EXISTS connector_registry_type ON connector_registry(type);
        CREATE INDEX IF NOT EXISTS connector_instances_connector_id ON connector_instances(connector_id);
    """

    ins_ds = """
        INSERT INTO datasets (id, dataset_id, type, name, validation_config, extraction_config, dedup_config, data_schema, denorm_config, router_config, dataset_config, tags, data_version, status, created_by, updated_by, created_date, updated_date, published_date) VALUES
        ('new-york-taxi-data', 'new-york-taxi-data', 'dataset', 'new-york-taxi-data', '{"validate": true, "mode": "Strict", "validation_mode": "Strict"}', '{"is_batch_event": false}', '{"drop_duplicates": true, "dedup_key": "tripID", "dedup_period": 604800}', '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"tripID":{"type":"string","suggestions":[{"message":"The Property tripID appears to be uuid format type.","advice":"Suggest to not to index the high cardinal columns","resolutionType":"DEDUP","severity":"LOW","path":"properties.tripID"}],"arrival_format":"text","data_type":"string"}},"additionalProperties":false}', '{}', '{"topic": "new-york-taxi-data"}', '{"data_key": "", "timestamp_key": "tpep_pickup_datetime", "exclude_fields": [], "entry_topic": "azure.ingest", "redis_db_host": "obsrv-dedup-redis-master.redis.svc.cluster.local", "redis_db_port": 6379, "index_data": true, "redis_db": 0}', '{}', '1', 'Live', 'SYSTEM', 'SYSTEM', '2024-03-27 06:48:35.993478', '2024-03-27 06:48:35.993478', '2024-03-27 06:48:35.993478');
    """

    # ins_ds = """
    #     INSERT INTO datasets (id, dataset_id, type, name, dataset_config, status, created_by, updated_by) VALUES
    #     ('new-york-taxi-data', 'new-york-taxi-data', 'dataset', 'new-york-taxi-data', '{"entry_topic": "test.ingest"}', 'Live', 'SYSTEM', 'SYSTEM');
    #     ('new-york-taxi-data', 'new-york-taxi-data', 'dataset', 'new-york-taxi-data', '{"entry_topic": "azure.ingest"}', 'Live', 'SYSTEM', 'SYSTEM');
    # """

    ins_cr = """
        INSERT INTO connector_registry (id, version, type, category, name, description, technology, licence, owner, iconURL, status, created_by, updated_by, updated_date) VALUES
        ('s3.1', '1', 'source', 'object', 'test_reader', 'test_reader', 'Python', 'Apache 2.0', 'ravi@obsrv.ai', 'http://localhost', 'Live', 'SYSTEM', 'SYSTEM', now()),
     ('azure.1', '1', 'source', 'object', 'test_reader', 'test_reader', 'Python', 'Apache 2.0', 'ravi@obsrv.ai', 'http://localhost', 'Live', 'SYSTEM', 'SYSTEM', now());
    """

    connector_config = json.dumps(
        {
        "source":{
            "type":"azure_blob",
            "credentials":{
                "account_name":azure_conf['accountName'],
                "account_key":azure_conf['accountKey']
            },
        "containername":"test-container",
        "blob_endpoint":azure_conf['blobEndpoint'],
        "prefix":"/"
        }})
    enc_config = enc.encrypt(connector_config)

    ins_ci = """
        INSERT INTO connector_instances (id, dataset_id, connector_id, connector_type, connector_config, operations_config, status, connector_state, connector_stats, created_by, updated_by, created_date, updated_date, published_date) VALUES
        ('s3.new-york-taxi-data.1', 'new-york-taxi-data', 's3.1', 'source', %s, '{}', 'Live', '{}', '{}', 'SYSTEM', 'SYSTEM', now(), now(), now()
        ),
  
        ('azure.new-york-taxi-data.1', 'new-york-taxi-data', 'azure.1', 'source', %s, '{}', 'Live', '{}', '{}', 'SYSTEM', 'SYSTEM', now(), now(), now()
        );
    """

    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "r"
    ) as config_file:
        config = yaml.safe_load(config_file)
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
        cur.execute(indexes)
        cur.execute(ins_ds)
        cur.execute(ins_cr)
        cur.execute(ins_ci, (json.dumps(enc_config),(json.dumps(enc_config))))

        conn.commit()
        conn.close()
