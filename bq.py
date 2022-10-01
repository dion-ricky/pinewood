from google.cloud import bigquery
from google.oauth2 import service_account


class BQClient:
    def __init__(self, keyfile_path) -> None:
        credentials = service_account.Credentials.from_service_account_file(keyfile_path)
        self._client = bigquery.Client(credentials=credentials)

    @property
    def client(self):
        return self._client
