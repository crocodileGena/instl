#!/usr/bin/env python3


import abc
import urllib.request, urllib.parse, urllib.error
import urllib.parse
import json

from configVar import var_stack

have_boto = True
try:
    import boto
except Exception:
    have_boto = False


class ConnectionBase(object):
    repo_connection = None # global singleton, holding current connection
    def __init__(self):
        pass

    def get_cookie(self, net_loc):
        retVal = None
        cookie_list = var_stack.ResolveVarToList("COOKIE_JAR", default=[])
        if cookie_list:
            for cookie_line in cookie_list:
                cred_split = cookie_line.split(":", 2)
                if len(cred_split) == 2 and net_loc.lower() == cred_split[0].lower():
                    retVal = 'Cookie', cred_split[1]
                    break

        return retVal

    def get_custom_headers(self, net_loc):
        retVal = list()
        cookie = self.get_cookie(net_loc)
        if cookie is not None:
            retVal.append(cookie)
        custom_headers = var_stack.ResolveVarToList("CUSTOM_HEADERS", default=[])
        if custom_headers:
            for custom_header in custom_headers:
                custom_header_split = custom_header.split(":", 1)
                header_net_loc, header_values = custom_header_split[0], custom_header_split[1]
                if header_net_loc.lower() == net_loc.lower():
                    try:
                        header_values = json.loads(header_values)
                    except Exception as ex:
                        print("CUSTOM_HEADERS not valid json", custom_headers, ex)
                    else:
                        retVal.extend(list(header_values.items()))
        return retVal

    @abc.abstractmethod
    def open_connection(self, credentials):
        pass

    @abc.abstractmethod
    def translate_url(self, in_bare_url):
        pass


class ConnectionHTTP(ConnectionBase):
    def __init__(self):
        super().__init__()

    def open_connection(self, credentials):
        pass

    @abc.abstractmethod
    def translate_url(self, in_bare_url):
        parsed = urllib.parse.urlparse(in_bare_url)
        quoted_results = urllib.parse.ParseResult(scheme=parsed.scheme, netloc=parsed.netloc, path=urllib.parse.quote(parsed.path, "$()/:%"), params=parsed.params, query=parsed.query, fragment=parsed.fragment)
        retVal = urllib.parse.urlunparse(quoted_results)
        return retVal

if have_boto:
    class ConnectionS3(ConnectionHTTP):
        def __init__(self, credentials):
            super().__init__()
            self.boto_conn = None
            self.open_bucket = None
            default_expiration_str = var_stack.ResolveVarToStr("S3_SECURE_URL_EXPIRATION", default=str(60*60*24))
            self.default_expiration =  int(default_expiration_str)  # in seconds
            self.open_connection(credentials)

        def open_connection(self, credentials):
            in_access_key, in_secret_key, in_bucket = credentials
            self.boto_conn = boto.connect_s3(in_access_key, in_secret_key)
            self.open_bucket = self.boto_conn.get_bucket(in_bucket, validate=False)
            var_stack.set_var("S3_BUCKET_NAME", "from command line options").append(in_bucket)

        def translate_url(self, in_bare_url):
            parseResult = urllib.parse.urlparse(in_bare_url)
            if parseResult.netloc.startswith(self.open_bucket.name):
                the_key = self.open_bucket.get_key(parseResult.path, validate=False)
                retVal = the_key.generate_url(self.default_expiration)
            else:
                retVal = super().translate_url(in_bare_url)
            return retVal


def connection_factory():
    if ConnectionBase.repo_connection is None:
        if "__CREDENTIALS__" in var_stack and have_boto:
            credentials = var_stack.ResolveVarToStr("__CREDENTIALS__")
            cred_split = credentials.split(":")
            if cred_split[0].lower() == "s3":
                ConnectionBase.repo_connection = ConnectionS3(cred_split[1:])
        else:
            ConnectionBase.repo_connection = ConnectionHTTP()
    return ConnectionBase.repo_connection


def translate_url(in_bare_url):
    translated_url = connection_factory().translate_url(in_bare_url)
    parsed = urllib.parse.urlparse(translated_url)
    cookie = connection_factory().get_custom_headers(parsed.netloc)
    return translated_url, cookie
