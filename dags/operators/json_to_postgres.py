from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class JsonToPostgresOperator(BaseOperator):
    """
    An Airflow operator to perform bulk insert of Valorant player data into a PostgreSQL table.

    This operator retrieves data from an XCom, which is expected to be a list of dictionaries,
    and inserts it into a specified PostgreSQL table using the psycopg2 `execute_values` method
    for efficient bulk inserts.
    """

    @apply_defaults
    def __init__(
            self,
            conn_id: str,
            table: str,
            xcom_task_id: str,
            *args, **kwargs
    ):
        """
        Initialize the JsonToPostgresOperator.

        :param conn_id: The Airflow connection ID for the PostgreSQL database.
        :type conn_id: str
        :param table: The name of the PostgreSQL table to insert data into.
        :type table: str
        :param xcom_task_id: The task ID of the task producing the XCom with data.
        :type xcom_task_id: str
        """
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.xcom_task_id = xcom_task_id

    def get_hook(self):
        """Retrieve the PostgreSQL hook."""
        return PostgresHook(postgres_conn_id=self.conn_id)

    def execute(self, context):
        """
        Execute the operator.

        This method pulls data from an XCom, transforms it into a format suitable for
        insertion into PostgreSQL, and performs the bulk insert.

        :param context: The context passed from the Airflow scheduler.
        :type context: dict
        """
        # Pull data from XCom
        xcom_data = context['ti'].xcom_pull(task_ids=self.xcom_task_id)

        # Establish database connection
        conn = self.get_hook().get_conn()
        cursor = conn.cursor()

        # Prepare the SQL insert query
        insert_query = f"""
        INSERT INTO {self.table} ({', '.join(xcom_data[0].keys())})
        VALUES %s;
        """

        # Convert the list of dictionaries into a list of tuples
        data_tuples = [tuple(d.values()) for d in xcom_data]

        # Use execute_values to perform the bulk insert
        execute_values(cursor, insert_query, data_tuples)

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
