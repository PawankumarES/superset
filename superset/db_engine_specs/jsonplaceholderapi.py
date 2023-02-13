from superset.db_engine_specs.sqlite import SqliteEngineSpec


class JsonPlaceHolderAPIEngineSpec(SqliteEngineSpec):
    """Engine for JsonPlaceHolderAPI tables"""

    engine = "jsonplaceholderapi"
    engine_name = "JsonPlaceHolderAPI"
    allows_joins = True
    allows_subqueries = True

    default_driver = "apsw"
    sqlalchemy_uri_placeholder = "jsonplaceholderapi://"