"""
This script uploads a local CSV file to an internal stage in Snowflake using Snowpark.
"""
import json

from snowflake.core import Root
from snowflake.snowpark import Session

DEFAULT_ENCRYPTION = 'SNOWFLAKE_SSE'
BOG_TRANSACTIONS_STAGE = 'bog_transactions_stage'
STAGE_DB = 'TRANSACTIONS_STAGE'
STAGE_SCHEMA = 'BOG'

with open('conn_config.json', encoding='UTF-8') as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()
root = Root(session)

my_stage_res = root.databases[STAGE_DB].schemas[STAGE_SCHEMA].stages[BOG_TRANSACTIONS_STAGE]
my_stage_res.put('6554164645.csv', '/')
