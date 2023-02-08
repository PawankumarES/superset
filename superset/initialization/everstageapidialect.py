from typing import Any, Dict, List, Optional, Tuple

from shillelagh.backends.apsw.dialects.base import APSWDialect
from sqlalchemy.engine import Connection
from superset.initialization.everstageapiadapter import EverstageAPI as adapter
from superset.initialization.everstageapiadapter import ev_oauth_token
import requests_cache

ADAPTER_NAME = 'everstageapi'

class EverstageAPIDialect(APSWDialect):

    """
    A SQLAlchemy dialect for EverstageAPI
    """

    name = "everstageapi"
    driver = "apsw"
    supports_statement_cache = True
    supports_sane_rowcount = False

    def __init__(
        self,
        **kwargs: Any,
    ):
        # We tell Shillelagh that this dialect supports just one adapter
        super().__init__(safe=True, adapters=[ADAPTER_NAME], **kwargs)

    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ):
        self._session = requests_cache.CachedSession(
            cache_name="everstage_cache",
            backend="sqlite",
            expire_after=180,
            )
        bearer_token = f'Bearer {ev_oauth_token}'
        headers = {"Authorization": bearer_token, 'Accept': 'application/json', 'Content-Type': 'application/json' } 
        url = f"http://localhost:3000/superset/get-datasheet-names"
        response = self._session.get(url, verify = False, headers = headers)
        print(f'response - {response.json()}')
        if response.ok:
            return response.json()['dsNames']
    
    def create_connect_args(
        self,
        url,
    ) -> Tuple[Tuple[()], Dict[str, Any]]:
        path = str(url.database) if url.database else ":memory:"
        return (), {
            "path": path,
            "adapters": self._adapters,
            "adapter_kwargs": self._adapter_kwargs,
            "safe": self._safe,
            "isolation_level": self.isolation_level,
        }
