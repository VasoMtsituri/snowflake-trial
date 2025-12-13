import json

from snowflake.snowpark import Session

with open("conn_config.json") as f:
    connection_parameters = json.load(f)

session = Session.builder.configs(connection_parameters).create()

merchants_categorized = session.table("MERCHANTS.PUBLIC.MERCHANTS_CATEGORIZED")
merchants_categorized.limit(10).show()
