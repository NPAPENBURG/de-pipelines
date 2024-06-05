import yaml
from airflow.models import Connection
from airflow import settings


def create_connections_from_yaml(yaml_file):
    """
    Create or update Airflow connections from a YAML file.

    This function reads connection details from the specified YAML file and adds them to
    the Airflow database. If a connection with the same `conn_id` already exists, it updates
    its parameters.

    """

    with open(yaml_file, 'r') as f:
        connections_data = yaml.safe_load(f)

    session = settings.Session()

    for connection_name, connection_params in connections_data.items():
        existing_conn = session.query(Connection).filter(Connection.conn_id == connection_name).first()

        if existing_conn:
            # If connection already exists, update its parameters
            existing_conn.conn_type = connection_params['conn_type']
            existing_conn.host = connection_params['host']
            existing_conn.login = connection_params.get('login', None)
            existing_conn.password = connection_params.get('password', None)
            existing_conn.schema = connection_params.get('schema', None)
            existing_conn.port = connection_params.get('port', None)
            existing_conn.extra = connection_params.get('extra', None)
        else:
            # If connection doesn't exist, create a new one
            new_conn = Connection(
                conn_id=connection_name,
                conn_type=connection_params['conn_type'],
                host=connection_params['host'],
                login=connection_params.get('login', None),
                password=connection_params.get('password', None),
                schema=connection_params.get('schema', None),
                port=connection_params.get('port', None),
                extra=connection_params.get('extra', None)
            )
            session.add(new_conn)

    session.commit()
