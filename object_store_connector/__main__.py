import os

from obsrv.connector.batch import SourceConnector

from connector import ObjectStoreConnector

# from obsrv.utils import Config

if __name__ == "__main__":
    connector = ObjectStoreConnector()
    # config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

    SourceConnector.process(connector=connector)
