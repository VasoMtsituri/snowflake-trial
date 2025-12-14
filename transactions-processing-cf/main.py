"""
A Cloud Function as a Snowpark script that reads transaction data from a source table,
apply some transformations, and writes the results to a destination table.
"""
import json
import logging

from snowflake.snowpark import Session
from snowflake.snowpark.functions import bround, monthname, year

from gcp_utils import read_gcp_secret

logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'project-6e4dc205-0d7d-44c6-975'
SECRET_ID = 'snowflake-user-config'
SOURCE_TABLE = 'TRANSACTIONS_RAW.BANKS.BOG'
DESTINATION_TABLE = 'TRANSACTIONS.BANKS.BOG'

def process_transactions(request) -> str:
    """
    Cloud Function entry point to process transactions.

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

    # Drop unnecessary columns
    transactions = transactions.drop(['COMMISSION_CCY', 'SETTLEMENT_CCY', 'BRAND_NAME'])

    # Round the settlement amount (column SETTLEMENT_AMOUNT) to 2 decimal places
    transactions = transactions.with_column('SETTLEMENT_AMOUNT',
                                            bround(transactions['SETTLEMENT_AMOUNT'], 2))

    # Extract month and year from SETTLEMENT_DATE
    transactions = transactions.with_column('TRANSACTION_MONTH',
                                            monthname(transactions['SETTLEMENT_DATE']))
    transactions = transactions.with_column('TRANSACTION_YEAR',
                                            year(transactions['SETTLEMENT_DATE']))

    # Write the transformed transactions data to the destination table
    transactions.write.mode('append').save_as_table(DESTINATION_TABLE)

    logging.info('Transactions processed successfully.')

    return 'OK'
