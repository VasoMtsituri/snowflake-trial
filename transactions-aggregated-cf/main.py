"""
A Cloud Function as a Snowpark script that reads transaction data from a source table,
apply some transformations, and writes the results to a destination table.
"""
import json
import logging

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum

from gcp_utils import read_gcp_secret

logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'project-6e4dc205-0d7d-44c6-975'
SECRET_ID = 'snowflake-user-config'
SOURCE_TABLE = 'TRANSACTIONS.BANKS.BOG'
DESTINATION_TABLE = 'TRANSACTIONS_PRS.BANKS.BOG_DAILY'

def aggregate_transactions(request) -> str:
    """
    Cloud Function entry point to aggregate transactions.

    Args:
        request (flask.Request): flask.Request object. Defined as convention for Cloud Functions.

    Returns:
        str: status message.
    """
    # Log the request body for info purposes
    request_body = request.get_json()
    logging.info(f'Request: {request_body}')

    # Read Snowflake connection details and create a Snowpark session
    connection_parameters = json.loads(read_gcp_secret(project_id=PROJECT_ID, secret_id=SECRET_ID))
    session = Session.builder.configs(connection_parameters).create()

    # Read source table
    transactions = session.table(SOURCE_TABLE)

    # Aggregate the data to get daily total settlement amount
    aggregated_transactions = transactions.group_by(
        col('SETTLEMENT_DATE').alias('TRANSACTION_DATE'),
        'TRANSACTION_YEAR',
        'TRANSACTION_MONTH',
        'OPERATION_TYPE'
    ).agg(
        sum(transactions['SETTLEMENT_AMOUNT']).alias('TOTAL_SETTLEMENT_AMOUNT'),
        count(transactions['RRN']).alias('TOTAL_TRANSACTIONS')
    )

    # Write the aggregated transactions data to the destination table
    aggregated_transactions.write.mode('append').save_as_table(DESTINATION_TABLE)

    logging.info('Transactions aggregated successfully.')

    return 'OK'
