from shillelagh.adapters.base import Adapter
import urllib
import requests_cache
from shillelagh.adapters.registry import registry
from shillelagh.lib import analyze
from urllib.parse import urlparse

class JsonPlaceHolderAPI(Adapter):

    """
    An adapter to data from https://jsonplaceholder.typicode.com/.
    """

    safe = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs):
        """
        Method which checks whether given uri could be handled by the adapter
        """
        parsed = urllib.parse.urlparse(uri)
        query_string = urllib.parse.parse_qs(parsed.query)
        return (
            parsed.netloc == "jsonplaceholder.typicode.com"
        )
    def __init__(self, table, postId: str):
        super().__init__()
        self.postId = postId
        self.table = table
        # using cache, since the adapter does a lot of similar API requests and the data rarely changes
        self._session = requests_cache.CachedSession(
            cache_name="jsonplaceholders_cache",
            backend="sqlite",
            expire_after=180,
            )
        # Set columns based on the result
        self._set_columns()

    @staticmethod
    def parse_uri(table:str):
        parsed = urlparse(table)
        query_string = urllib.parse.parse_qs(parsed.query)
        postId = query_string["postId"][0]
        # Here we are targetting postId to be filterable
        return (parsed.path, postId,)

    # @staticmethod
    # def parse_uri(uri: str):
    #     return uri

    # def __init__(self, uri: str):
    #     """
    #     Instantiate the adapter.

    #     Here ``uri`` will be passed from the ``parse_uri`` method
    #     """
    #     super().__init__()

    #     parsed = urllib.parse.urlparse(uri)
    #     query_string = urllib.parse.parse_qs(parsed.query)

    #     self.postId = query_string["postId"][0]
    #     self._session = requests_cache.CachedSession(
    #         cache_name="jsonplaceholders_cache",
    #         backend="sqlite",
    #         expire_after=180,
    #         )

    
    def get_data(
        self,
        bounds,
        order,
        **kwargs,
    ):
        url = f"https://jsonplaceholder.typicode.com/{self.table}"
        params = {"postId": self.postId}
        response = self._session.get(url, params=params)
        if response.ok:
            return response.json()
    
    def _set_columns(self):
        rows = list(self.get_data({}, []))
        column_names = list(rows[0].keys()) if rows else []

        _, order, types = analyze(iter(rows))

        self.columns = {
            column_name: types[column_name](
                filters=[],
                order=order[column_name],
                exact=False,
            )
            for column_name in column_names
        }

    def get_columns(self):
        return self.columns