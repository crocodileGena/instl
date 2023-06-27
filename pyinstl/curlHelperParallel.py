#!/usr/bin/env python3.9
import os
import abc
import itertools
import re
import subprocess
import sys
from distutils.version import StrictVersion
from pathlib import PurePath
from re import compile, IGNORECASE
import functools
import logging

log = logging.getLogger()

import utils
from configVar import config_vars  # √
from . import connectionBase
from pybatch import AnonymousAccum, Progress, MakeDir


# TODO IdanMZ Add documentation, add lazy loading,
# use wrapper instead of static creation?
class CUrlHelperParallel(object, metaclass=abc.ABCMeta):
    """ Create download commands. Each function should be overridden to implement the download
        on specific platform using a specific copying tool. All functions return
        a list of commands, even if there is only one. This will allow to return
        multiple commands if needed.
    """
    curl_write_out_str = r'%{url_effective}, %{size_download} bytes, %{time_total} sec., %{speed_download} bps.\n'
    # for debugging:
    curl_extra_write_out_str = r'    num_connects:%{num_connects}, time_namelookup: %{time_namelookup}, time_connect: %{time_connect}, time_pretransfer: %{time_pretransfer}, time_redirect: %{time_redirect}, time_starttransfer: %{time_starttransfer}\n\n'
    cached_is_supported = None # Lazy loading
    min_supported_curl_version = "7.66.0"

    def __init__(self) -> None:
        self.urls_to_download = list()
        self.urls_to_download_last = list()
        self.short_win_paths_cache = dict()

    def add_download_url(self, url, path, verbatim=False, size=0, download_last=False):
        if verbatim:
            translated_url = url
        else:
            translated_url = connectionBase.connection_factory(config_vars).translate_url(url)
        if download_last:
            self.urls_to_download_last.append((translated_url, path, size))
        else:
            self.urls_to_download.append((translated_url, path, size))

    def get_num_urls_to_download(self):
        return len(self.urls_to_download)+len(self.urls_to_download_last)

    def download_from_config_file(self, config_file):
        pass

    def fix_path(self, in_some_path_to_fix):
        """  On Windows: to overcome cUrl inability to handle path with unicode chars, we try to calculate the windows
                short path (DOS style 8.3 chars). The function that does that, win32api.GetShortPathName,
                does not work for paths that do not yet exist so we need to also create the folder.
                However, if the creation requires admin permissions - it could fail -
                in which case we revert to using the long path.
        """

        fixed_path = PurePath(in_some_path_to_fix)
        if 'Win' in utils.get_current_os_names():
            # to overcome cUrl inability to handle path with unicode chars, we try to calculate the windows
            # short path (DOS style 8.3 chars). The function that does that, win32api.GetShortPathName,
            # does not work for paths that do not yet exist, so we need to also create the folder.
            # However, if the creation requires admin permissions - it could fail -
            # in which case we revert to using the long path.
            import win32api
            fixed_path_parent = str(fixed_path.parent)
            fixed_path_name = str(fixed_path.name)
            if fixed_path_parent not in self.short_win_paths_cache:
                try:
                    os.makedirs(fixed_path_parent, exist_ok=True)
                    short_parent_path = win32api.GetShortPathName(fixed_path_parent)
                    self.short_win_paths_cache[fixed_path_parent] = short_parent_path
                except Exception as e:  # failed to mkdir or get the short path? never mind, just use the full path
                    self.short_win_paths_cache[fixed_path_parent] = fixed_path_parent
                    log.warning(f"""warning creating short path failed for {fixed_path}, {e}, using long path""")

            short_file_path = os.path.join(self.short_win_paths_cache[fixed_path_parent], fixed_path_name)
            fixed_path = short_file_path.replace("\\", "\\\\")
        return fixed_path

    def create_config_files(self, curl_config_file_path, num_config_files):
        file_name_list = list()

        if self.get_num_urls_to_download() > 0:
            actual_num_config_files = 1 if CUrlHelperParallel.is_supported() else int(
                max(0, min(len(self.urls_to_download), num_config_files)))
            if self.urls_to_download_last:
                actual_num_config_files += 1
            num_digits = max(len(str(actual_num_config_files)), 2)
            file_name_list = ["-".join((os.fspath(curl_config_file_path), str(file_i).zfill(num_digits))) for file_i
                              in range(actual_num_config_files)]

            # open the files make sure they have r/w permissions and are utf-8
            wfd_list = list()
            for file_name in file_name_list:
                wfd = utils.utf8_open_for_write(file_name, "w")
                wfd_list.append(wfd)

            # write the header in each file
            for wfd in wfd_list:
                basename = os.path.basename(wfd.name)
                file_header_text = self.get_config_header(basename)
                wfd.write(file_header_text)

            last_file = None
            if self.urls_to_download_last:
                last_file = wfd_list.pop()

            def url_sorter(l, r):
                """ smaller files should be downloaded first so the progress bar gets moving early. """
                return l[2] - r[2]  # non Info.xml files are sorted by size

            wfd_cycler = itertools.cycle(wfd_list)
            url_num = 0
            sorted_by_size = self.urls_to_download # sorted(self.urls_to_download, key=functools.cmp_to_key(url_sorter))  # TODO IdanMZ - why?
            for url, path, size in sorted_by_size:
                fixed_path = self.fix_path(path)
                wfd = next(wfd_cycler)
                wfd.write(f'''url = "{url}"\noutput = "{fixed_path}"\n\n''')
                url_num += 1

            for wfd in wfd_list:
                wfd.close()

            for url, path, size in self.urls_to_download_last:
                fixed_path = self.fix_path(path)
                last_file.write(f'''url = "{url}"\noutput = "{fixed_path}"\n\n''')
                url_num += 1

            # insert None which means "wait" before the config file that downloads urls_to_download_last. but only if
            # there were actually download files other than urls_to_download_last. it might happen that there are
            # Note!
            # only urls_to_download_last - so no need to "wait". if we use the embedded parallel option of curl -
            # there will only be 2 files to execute the wait is built-in
            if last_file and len(wfd_list) > 0 and not CUrlHelperParallel.is_supported():
                file_name_list.insert(-1, None)

        return file_name_list

    # TODO IdanMZ implement
    @staticmethod
    def is_supported():
        if CUrlHelperParallel.cached_is_supported is None:
            try:
                CUrlHelperParallel.cached_is_supported = False

                #exe_name = config_vars.resolve_str("$(DOWNLOAD_TOOL_PATH)") # TODO IDanMZ talk to shai - this happens way too early
                exe_name = config_vars.resolve_str("curl")  # TODO IDanMZ talk to shai - this happens way too early
                # The curl --version output is
                # curl 7.79.1 (x86_64-apple-darwin21.0) libcurl/7.79.1 (SecureTransport) LibreSSL/3.3.6 zlib/1.2.11 nghttp2/1.45.1
                # Release-Date: 2021-09-22
                # Protocols: dict file ftp ftps gopher gophers http https imap imaps ldap ldaps mqtt pop3 pop3s rtsp smb smbs smtp smtps telnet tftp
                # Features: alt-svc AsynchDNS GSS-API HSTS HTTP2 HTTPS-proxy IPv6 Kerberos Largefile libz MultiSSL NTLM NTLM_WB SPNEGO SSL UnixSockets
                # So we take the fist line after the "curl" word

                proc = subprocess.Popen(
                    f"{exe_name} --version",
                    shell=True,
                    stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    universal_newlines=True)

                match = re.search(r"curl\s+([0-9.]+)\s", proc.stdout.read())
                if match is not None and len(match.groups()) > 0:
                    curl_version = StrictVersion(match.group(1))
                    min_version = StrictVersion(CUrlHelperParallel.min_supported_curl_version)
                    if min_version > curl_version:
                        log.info(f"legacy curl version {match.group(1)}")
                    else:
                        CUrlHelperParallel.cached_is_supported = True

            except Exception as e:
                log.info(f"Unable to detect version of curl, assuming legacy version.")
                pass

        # probably "$(DOWNLOAD_TOOL_PATH)" --version
        return CUrlHelperParallel.cached_is_supported

    def stderr_parser(self, max_files, previous_count = 0):
        r = compile(r'[0-9-]+\s+[0-9-]+\s+([a-z0-9.]+)\s+0\s+(\d+).+--:--:--\s+([0-9a-z.]+)\s*$', IGNORECASE)

        max_files = str(max_files) if max_files is not None else "..."

        def parser(line):
            m = r.findall(line)
            if len(m) > 0 and len(m[0]) > 2:
                downloaded_size, downloaded_files, download_speed = m[0]
                downloaded_files = str(int(downloaded_files) + previous_count)
                log.info(f"Progress: ... of ...; Downloading {downloaded_files} of {max_files}, Downloaded {downloaded_size}, Speed {download_speed}.")

        return parser


    def get_config_header(self, basename):
        sync_urls_cookie = str(config_vars.get("COOKIE_FOR_SYNC_URLS", ""))
        connect_time_out = str(config_vars.setdefault("CURL_CONNECT_TIMEOUT", "16"))
        max_time = str(config_vars.setdefault("CURL_MAX_TIME", "180"))
        retries = str(config_vars.setdefault("CURL_RETRIES", "2"))
        retry_delay = str(config_vars.setdefault("CURL_RETRY_DELAY", "8"))
        cookie_text = f"cookie = {sync_urls_cookie}\n" if sync_urls_cookie else ""

        if CUrlHelperParallel.is_supported():
            verbosity = "progress-bar"
            write_out = ""
            parallel = "parallel"
        else:
            verbosity = "silent"
            write_out = f"Progress: ... of ...; {basename}: {self.curl_write_out_str}"
            parallel = ""

        return  f"""
insecure
raw
fail
{verbosity}
{parallel}
show-error
compressed
create-dirs
connect-timeout = {connect_time_out}
max-time = {max_time}
retry = {retries}
retry-delay = {retry_delay}
{cookie_text}
{write_out}
"""

    # same
    def get_normalized_path(self, config_file):
        # curl on windows has problem with path to config files that have unicode characters
        return win32api.GetShortPathName(config_file) if sys.platform == 'win32' else config_file

    def get_download_commands(self):
        dl_commands = AnonymousAccum()

        main_outfile = config_vars["__MAIN_OUT_FILE__"].Path()
        curl_config_folder = main_outfile.parent.joinpath(main_outfile.name + "_curl_p")
        MakeDir(curl_config_folder, chowner=True, own_progress_count=0, report_own_progress=False)()
        curl_config_file_path = curl_config_folder.joinpath(config_vars["CURL_CONFIG_FILE_NAME"].str())

        # num_config_files = int(config_vars["PARALLEL_SYNC"])
        num_config_files = 1

        dl_commands += Progress("Downloading with 1 process (Parallel)")
        config_file_list = self.create_config_files(curl_config_file_path, num_config_files)

        num_files_to_download = int(config_vars["__NUM_FILES_TO_DOWNLOAD__"])

        # Download using combined file
        for index, config_file in enumerate(config_file_list):
            exe_name = config_vars.resolve_str("$(DOWNLOAD_TOOL_PATH)")
            parser = self.stderr_parser(self.get_num_urls_to_download(),
                                                         0 if index == 0 else len(self.urls_to_download))

            proc = subprocess.Popen(
                f"{exe_name} --config '{self.get_normalized_path(config_file)}'",
                shell=True,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            # Read and process the stderr output line by line
            for line in proc.stderr:
                parser(line)

            proc.wait()  # Wait for the process to complete

            # dl_commands += Subprocess("$(DOWNLOAD_TOOL_PATH)", "--config", self.get_normalized_path(config_file),
            #                       stderr_means_err=False, stderr_parser=self.instlObj.dl_tool.stderr_parser)
        return list()
