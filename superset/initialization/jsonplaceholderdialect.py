from typing import Any, Dict, List, Optional, Tuple

from shillelagh.backends.apsw.dialects.base import APSWDialect
from sqlalchemy.engine import Connection

ADAPTER_NAME = 'jsonplaceholderapi'

class JsonPlaceHolderDialect(APSWDialect):

    """
    A SQLAlchemy dialect for JsonPlaceHolderAPI
    """

    name = "jsonplaceholderapi"
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
        return ['comments']
    
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
