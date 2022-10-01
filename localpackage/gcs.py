from google.cloud import storage
from google.oauth2 import service_account


class GCSClient:
    def __init__(self, keyfile_path) -> None:
        credentials = service_account.Credentials.from_service_account_file(keyfile_path)
        self._client = storage.Client(credentials=credentials)

    @property
    def client(self):
        return self._client