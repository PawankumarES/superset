from superset.db_engine_specs.sqlite import SqliteEngineSpec


class EverstageAPIEngineSpec(SqliteEngineSpec):
    """Engine for EverstageAPI tables"""

    engine = "everstageapi"
    engine_name = "EverstageAPI"
    allows_joins = True
    allows_subqueries = True

    default_driver = "apsw"
    sqlalchemy_uri_placeholder = "everstage://"