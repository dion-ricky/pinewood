import re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'


class GDriveClient:
    def __init__(self, keyfile_path) -> None:
        credentials = service_account.Credentials.from_service_account_file(keyfile_path)
        credentials.with_scopes(SCOPES)

        self._service = build('drive', 'v3', credentials=credentials)

    @property
    def service(self):
        return self._service

    def list_file(self, **kwargs):
        page_token = None
        while True:
            response = self.service.files() \
                        .list(
                            **kwargs,
                            fields='nextPageToken, files(id, name)',
                            pageToken=page_token).execute()

            for file in response.get('files', []):
                yield file

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    def get_file_by_regex(self, re_pattern):
        for file in self.list_file():
            if re.findall(re_pattern, file.get('name')) != []:
                return file
        return None

    def download(self, file_id):
        request = self.service.files().get_media(fileId=file_id)
        return request

    def downloader(self, file_id, destination_path):
        request = self.download(file_id)
        file = open(destination_path, 'wb')
        _downloader = MediaIoBaseDownload(file, request)
        done = False

        while not done:
            status, done = _downloader.next_chunk()

        file.close()
        return destination_path