import json

from snowflake.snowpark import Session

with open("conn_config.json") as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()

transactions = session.table("TRANSACTIONS_RAW.BANKS.TRANSACTIONS")
transactions_count = transactions.group_by(transactions.ID).count()

transactions_count.write.mode("overwrite").save_as_table("TRANSACTIONS_RAW.BANKS.TRANSACTIONS_COUNTED_FROM_CF")
