"""
A Cloud Function as a Snowpark script that reads transaction data from a source table,
apply some transformations, and writes the results to a destination table.
"""
import json
import logging

from snowflake.snowpark import Session

from gcp_cf.gcp_utils import read_gcp_secret

logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'project-6e4dc205-0d7d-44c6-975'
SECRET_ID = 'snowflake-user-config'
SOURCE_TABLE = 'TRANSACTIONS_RAW.BANKS.TRANSACTIONS'
DESTINATION_TABLE = 'TRANSACTIONS_RAW.BANKS.TRANSACTIONS_COUNTED_FROM_CF'

def process_transactions(request) -> str:
    """
    Cloud Function entry point to process transactions.

    Args:
        request (flask.Request): flask.Request object. Defined as convention for Cloud Functions.

    Returns:
        str: status message.
    """
    request_body = request.get_json()
    logging.info(f'Request: {request_body}')
    connection_parameters = json.loads(read_gcp_secret(project_id=PROJECT_ID, secret_id=SECRET_ID))
    session = Session.builder.configs(connection_parameters).create()

    transactions = session.table(SOURCE_TABLE)
    transactions_count = transactions.group_by(transactions.ID).count()

    transactions_count.write.mode("overwrite").save_as_table(DESTINATION_TABLE)
    logging.info('Transactions processed successfully.')

    return 'OK'
