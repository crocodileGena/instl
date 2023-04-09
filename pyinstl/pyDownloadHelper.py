import abc
from pathlib import PurePath
from configVar import config_vars
from . import connectionBase


class PyDownloadHelper(object, metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        self.urls_to_download = list()

    def add_download_url(self, url, path, verbatim=False, size=0, download_last=False):
        if verbatim:
            translated_url = url
        else:
            translated_url = connectionBase.connection_factory(config_vars).translate_url(url)
        self.urls_to_download.append((translated_url, path, size))

    def get_num_urls_to_download(self):
        return len(self.urls_to_download)

    def get_urls_to_download(self):
        return self.urls_to_download

    def download_from_config_file(self, config_file):
        pass

    def fix_path(self, in_some_path_to_fix):
        fixed_path = PurePath(in_some_path_to_fix)
        return fixed_path

    def create_config_files(self, curl_config_file_path, num_config_files):
        return []