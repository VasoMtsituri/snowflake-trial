"""
A Snowpark script that reads transaction data from a source table,
apply some cleaning and enrichment. Finally writes the results to a destination table.
"""
import json

from snowflake.snowpark import Session
from snowflake.snowpark.functions import bround, monthname, year

SOURCE_TABLE = 'TRANSACTIONS_RAW.BANKS.BOG'
DESTINATION_TABLE = 'TRANSACTIONS.BANKS.BOG'

with open('conn_config.json', encoding='UTF-8') as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()

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
