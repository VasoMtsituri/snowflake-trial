"""
A Snowpark script that reads transaction data from a source table,
apply some transformations, and writes the results to a destination table.
"""
import json

from snowflake.snowpark import Session

SOURCE_TABLE = 'TRANSACTIONS_RAW.BANKS.TRANSACTIONS'
DESTINATION_TABLE = 'TRANSACTIONS_RAW.BANKS.TRANSACTIONS_COUNTED_FROM_CF'

with open('conn_config.json', encoding='UTF-8') as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()

transactions = session.table(SOURCE_TABLE)
transactions_count = transactions.group_by(transactions.ID).count()

transactions_count.write.mode('overwrite').save_as_table(DESTINATION_TABLE)
