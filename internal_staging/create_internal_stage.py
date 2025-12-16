"""
Creates an internal stage in Snowflake for BOG transactions with default encryption.
"""
import json

from snowflake.core import Root
from snowflake.core.stage import Stage, StageEncryption
from snowflake.snowpark import Session

DEFAULT_ENCRYPTION = 'SNOWFLAKE_SSE'
BOG_TRANSACTIONS_STAGE = 'bog_transactions_stage'
STAGE_DB = 'TRANSACTIONS_STAGE'
STAGE_SCHEMA = 'BOG'

with open('conn_config.json', encoding='UTF-8') as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()
root = Root(session)

bog_transactions_stage = Stage(
    name=BOG_TRANSACTIONS_STAGE,
    encryption=StageEncryption(type=DEFAULT_ENCRYPTION)
)

stages = root.databases[STAGE_DB].schemas[STAGE_SCHEMA].stages
stages.create(bog_transactions_stage)
