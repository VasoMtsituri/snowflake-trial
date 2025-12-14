"""
A Snowpark script that reads cleaned transactions table from a source and applies some aggregations.
Finally writes the results to a destination table.
"""
import json

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum

SOURCE_TABLE = 'TRANSACTIONS.BANKS.BOG'
DESTINATION_TABLE = 'TRANSACTIONS_PRS.BANKS.BOG_DAILY'

with open('conn_config.json', encoding='UTF-8') as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()

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
