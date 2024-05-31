import yaml
from airflow import settings
from airflow.models import Connection


def create_connections_from_yaml(yaml_file):
    """
    Create Airflow connections from a YAML file.

    This function reads connection details from the specified YAML file and adds them to
    the Airflow database.

    :param yaml_file: The path to the YAML file containing connection details.
    :type yaml_file: str
    """

    with open(yaml_file, 'r') as f:
        connections_data = yaml.safe_load(f)

    for connection_name, connection_params in connections_data.items():
        conn = Connection(
            conn_id=connection_name,
            conn_type=connection_params['conn_type'],
            host=connection_params['host'],
            login=connection_params.get('login', None),
            password=connection_params.get('password', None),
            schema=connection_params.get('schema', None),
            port=connection_params.get('port', None),
            extra=connection_params.get('extra', None)
        )
        session = settings.Session()
        session.add(conn)
        session.commit()
