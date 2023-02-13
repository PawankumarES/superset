from shillelagh.adapters.base import Adapter
import urllib
import requests_cache
from shillelagh.adapters.registry import registry
from shillelagh.lib import analyze # Used to get column types, order and num of rows
from shillelagh.fields import Boolean, Field, Float, Integer, Order, String, StringDate
from urllib.parse import urlparse

ev_oauth_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im9oSW92LWdxWVluZVR4Szg1S2pTeSJ9.eyJodHRwczovL2V2ZXJzdGFnZS5jb20vcm9sZXMiOlsiRXZlcnN0YWdlQWRtaW4iLCJFdmVyc3RhZ2VVc2VyIiwiUG93ZXJBZG1pbiIsIlN1cGVyQWRtaW4iXSwiaHR0cHM6Ly9ldmVyc3RhZ2UuY29tL2VtYWlsIjoic3VwZXIuYWRtaW5AY3JpY2suY29tIiwiaXNzIjoiaHR0cHM6Ly9ldmVyc3RhZ2UtZGV2LnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJhdXRoMHw1ZjYxMmU2ZTAwMjc2NTAwNzEyOTY1YmIiLCJhdWQiOlsiaHR0cHM6Ly9ldmVyc3RhZ2UtaWNtIiwiaHR0cHM6Ly9ldmVyc3RhZ2UtZGV2LnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NzYyNzExMTMsImV4cCI6MTY3NjM1NzUxMywiYXpwIjoiNnRDUU1nVTRWTU5KeXZhaFZ0MFB5eG9SeHRnTWlUSFMiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIGNyZWF0ZTp1c2VyIiwicGVybWlzc2lvbnMiOlsiY3JlYXRlOmZpbkFkbWluIiwiY3JlYXRlOm9wc0FkbWluIiwiY3JlYXRlOnN1cGVyQWRtaW4iLCJjcmVhdGU6dXBzdHJlYW1EYXRhIiwiY3JlYXRlOnVzZXIiLCJlZGl0OmFsbFN0YXRlbWVudEJ1aWxkZXIiLCJlZGl0OmRhdGFmZWVkIiwiZWRpdDpkcmF3IiwiZWRpdDpEUlMiLCJlZGl0Om1hbnVhbEFkanVzdG1lbnRzIiwiZWRpdDpwbGFuIiwiZWRpdDpxdW90YSIsImVkaXQ6c2V0dGluZ3MiLCJlZGl0OnN5bmMiLCJlZGl0OnRlYW0iLCJlZGl0OnVzZXIiLCJlZGl0OnVzZXJHcm91cCIsInJlYWQ6ZGF0YXN5bmMiLCJyZWFkOmV2ZXJzdGFnZWFkbWluIiwicmVhZDp1c2VyR3JvdXAiLCJ2aWV3OmFkbWluRGFzaGJvYXJkIiwidmlldzphbGxEYXRhT3ZlcnZpZXciLCJ2aWV3OmFsbERyYXciLCJ2aWV3OmFsbERSUyIsInZpZXc6YWxsUGF5ZWVQcm9maWxlIiwidmlldzphbGxRdW90YSIsInZpZXc6YWxsU3RhdGVtZW50QnVpbGRlciIsInZpZXc6YWxsVGVhbXMiLCJ2aWV3OmF1ZGl0IiwidmlldzpkYXRhZmVlZCIsInZpZXc6ZXZlcnN0YWdlIiwidmlldzptYW51YWxBZGp1c3RtZW50cyIsInZpZXc6b3duRGF0YU92ZXJ2aWV3Iiwidmlldzpvd25EcmF3Iiwidmlldzpvd25EUlMiLCJ2aWV3Om93blBheWVlUHJvZmlsZSIsInZpZXc6b3duUXVvdGEiLCJ2aWV3Om93blRlYW0iLCJ2aWV3OnBheURldGFpbHMiLCJ2aWV3OnBheWVlRGFzaGJvYXJkIiwidmlldzpwbGFuIiwidmlldzpzdWJzY3JpcHRpb24iLCJ2aWV3OnVzZXJJbXBlcnNvbmF0aW9uIiwidmlldzp1c2VyU3VtbWFyeSIsInZpZXc6d2hhdGlmIiwid3JpdGU6ZGF0YXN5bmMiLCJ3cml0ZTpldmVyc3RhZ2VhZG1pbiJdfQ.gjJoR6H3PeGncMdM6-DCHqf5xoH_X1dRBpyma5B8D3FuCt8ZF_M7CabsXxszu_v6eSe2_rdYM0nQzIUJTTUDbjQPRJ-1ZMPUDPx1rbMkXVXa3CooRXZ4I3zi1KCt8NNZ0yR5lv4Co0M1Ga0JQGw4zgK8xvKuvBeHfaLiErqmrXLY_RVBBwCR_BdRvJP8Akergs55REC7hgDCRPyhISoxitPPYr2S8lZlQP-X3zCI3vUR6NmIlNUneZBiQeb_7JZ-Lp4NXYGLh1BamiJXfv3n1Q-22EI5WUjJmZZuYwFS5KYh4k16AqTjQ8Q6GNBeH35n4iH5f0nJ1_OUHfhBpENaXw"
class EverstageAPI(Adapter):

    """
    An adapter to data from https://localhost:6000/.
    """

    safe = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs):
        """
        Method which checks whether given uri could be handled by the adapter
        """
        # parsed = urllib.parse.urlparse(uri)
        # query_string = urllib.parse.parse_qs(parsed.query)
        # return (
        #     parsed.netloc == "jsonplaceholder.typicode.com"
        # )
        return True

    def __init__(self, table, client, db_name, ds_name):
        super().__init__()
        self.table = table
        self.client = client
        self.db_name = db_name
        self.ds_name = ds_name
        # using cache, since the adapter does a lot of similar API requests and the data rarely changes
        self._session = requests_cache.CachedSession(
            cache_name="everstage_cache",
            backend="sqlite",
            expire_after=180,
            )
        # Set columns based on the result
        self._set_columns()

    @staticmethod
    def parse_uri(table:str):
        parsed = urlparse(table)
        # query_string = urllib.parse.parse_qs(parsed.query)
        # ds_name = query_string.get('ds_name')
        db_ds_list = parsed.path.split('~')
        print(f'db_ds_list - {db_ds_list}')
        client = db_ds_list[0]
        db_name = db_ds_list[1]
        ds_name = db_ds_list[2]
        return (parsed.path, client, db_name, ds_name)

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
        is_set_col = False,
        **kwargs,
    ):
        url = f"http://localhost:3000/superset/get-datasheet-data"
        params = {"client" : self.client, "db_name" : self.db_name, "ds_name": self.ds_name}
        print(f'PARAMS - {params}')
        bearer_token = f'Bearer {ev_oauth_token}'
        headers = {"Authorization": bearer_token, 'Accept': 'application/json', 'Content-Type': 'application/json' }
        response = self._session.get(url, params=params, headers = headers)
        #response = self._session.get(url)
        res = response.json()
        #print(f'Res = {response.json()}')
        if response.ok:
            #return response.json()
            if not is_set_col:
                return res['data'][1:]
            if is_set_col:
                return res['data'][:1]

    
    # def get_datasheet_names(self):
    #     url = f"https://localhost:3000/superset/get-datasheet-names"
    #     bearer_token = f'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im9oSW92LWdxWVluZVR4Szg1S2pTeSJ9.eyJodHRwczovL2V2ZXJzdGFnZS5jb20vcm9sZXMiOlsiRXZlcnN0YWdlQWRtaW4iLCJFdmVyc3RhZ2VVc2VyIiwiUG93ZXJBZG1pbiIsIlN1cGVyQWRtaW4iXSwiaHR0cHM6Ly9ldmVyc3RhZ2UuY29tL2VtYWlsIjoic3VwZXIuYWRtaW5AY3JpY2suY29tIiwiaXNzIjoiaHR0cHM6Ly9ldmVyc3RhZ2UtZGV2LnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJhdXRoMHw1ZjYxMmU2ZTAwMjc2NTAwNzEyOTY1YmIiLCJhdWQiOlsiaHR0cHM6Ly9ldmVyc3RhZ2UtaWNtIiwiaHR0cHM6Ly9ldmVyc3RhZ2UtZGV2LnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NzU2NjkwOTcsImV4cCI6MTY3NTc1NTQ5NywiYXpwIjoiNnRDUU1nVTRWTU5KeXZhaFZ0MFB5eG9SeHRnTWlUSFMiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIGNyZWF0ZTp1c2VyIiwicGVybWlzc2lvbnMiOlsiY3JlYXRlOmZpbkFkbWluIiwiY3JlYXRlOm9wc0FkbWluIiwiY3JlYXRlOnN1cGVyQWRtaW4iLCJjcmVhdGU6dXBzdHJlYW1EYXRhIiwiY3JlYXRlOnVzZXIiLCJlZGl0OmFsbFN0YXRlbWVudEJ1aWxkZXIiLCJlZGl0OmRhdGFmZWVkIiwiZWRpdDpkcmF3IiwiZWRpdDpEUlMiLCJlZGl0Om1hbnVhbEFkanVzdG1lbnRzIiwiZWRpdDpwbGFuIiwiZWRpdDpxdW90YSIsImVkaXQ6c2V0dGluZ3MiLCJlZGl0OnN5bmMiLCJlZGl0OnRlYW0iLCJlZGl0OnVzZXIiLCJlZGl0OnVzZXJHcm91cCIsInJlYWQ6ZGF0YXN5bmMiLCJyZWFkOmV2ZXJzdGFnZWFkbWluIiwicmVhZDp1c2VyR3JvdXAiLCJ2aWV3OmFkbWluRGFzaGJvYXJkIiwidmlldzphbGxEYXRhT3ZlcnZpZXciLCJ2aWV3OmFsbERyYXciLCJ2aWV3OmFsbERSUyIsInZpZXc6YWxsUGF5ZWVQcm9maWxlIiwidmlldzphbGxRdW90YSIsInZpZXc6YWxsU3RhdGVtZW50QnVpbGRlciIsInZpZXc6YWxsVGVhbXMiLCJ2aWV3OmF1ZGl0IiwidmlldzpkYXRhZmVlZCIsInZpZXc6ZXZlcnN0YWdlIiwidmlldzptYW51YWxBZGp1c3RtZW50cyIsInZpZXc6b3duRGF0YU92ZXJ2aWV3Iiwidmlldzpvd25EcmF3Iiwidmlldzpvd25EUlMiLCJ2aWV3Om93blBheWVlUHJvZmlsZSIsInZpZXc6b3duUXVvdGEiLCJ2aWV3Om93blRlYW0iLCJ2aWV3OnBheURldGFpbHMiLCJ2aWV3OnBheWVlRGFzaGJvYXJkIiwidmlldzpwbGFuIiwidmlldzpzdWJzY3JpcHRpb24iLCJ2aWV3OnVzZXJJbXBlcnNvbmF0aW9uIiwidmlldzp1c2VyU3VtbWFyeSIsInZpZXc6d2hhdGlmIiwid3JpdGU6ZGF0YXN5bmMiLCJ3cml0ZTpldmVyc3RhZ2VhZG1pbiJdfQ.F6324HAA6bi_jXAMWpRAu19sSxU2lS7rQy6jsW9MTf0oo81AXcL0ZqnD4a-BlYn3GElu4j655lZ-WDGA33uZa5UkAFD0vPKFXI33KG9aCheJYpG7TAUIj4R8UHfUEDjtSeb0eaHTyM1-xJK9_ylnwga12K12KuoFW6TVA4kqsKQVf46LPtokXendNn4lWIb9Sf9-zUEDa-zbS_hGShJ6GUc5tYCD-uOoyttWb5Y3HPFjtaSEHRjq2emS99-c1ntu0F4dDTvdbIv1lKlGIMd34_sCFkyXUJS5KtFwKM56s-iOqH-KFypPaT1SyDFNOwxe-IpDqHSFa2qIyrd0Bj1L4w'
    #     headers = {"Authorization": bearer_token, 'Accept': 'application/json', 'Content-Type': 'application/json' }
    #     response = self._session.get(url, headers = headers)
    #     if response.ok:
    #         return response

    def _set_columns(self):
        # rows = list(self.get_data({}, []))
        # column_names = list(rows[0].keys()) if rows else []

        # _, order, types = analyze(iter(rows))

        # self.columns = {
        #     column_name: types[column_name](
        #         filters=[],
        #         order=order[column_name],
        #         exact=False,
        #     )
        #     for column_name in column_names
        # }
        var_dict = self.get_data({},[], is_set_col=True)
        var_dict = var_dict[0]
        column_names = var_dict.keys()
        self.columns = {}
        for name in column_names:
            if var_dict[name] in ['String', 'Email']:
                self.columns.update({name:String(filters = [])})
            elif var_dict[name] in ['Integer', 'Percentage']:
                self.columns.update({name:Float(filters = [])})
            elif var_dict[name] in  ['Boolean']:
                self.columns.update({name:Boolean(filters = [])})
            elif var_dict[name] in ['Date']:
                self.columns.update({name:StringDate(filters = [])})
            else:
                self.columns.update({name : String(filters = [])})
            
    def get_columns(self):
        return self.columns