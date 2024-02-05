import requests
from requests_toolbelt import sessions

from .cwms_ts import CwmsTsMixin
from .cwms_loc import CwmsLocMixin


class CWMS(CwmsLocMixin, CwmsTsMixin): #CwmsLevelMixin, CwmsSecMixin):
    
    def __init__(self, conn=None):
        self.conn = conn
        
    def connect(self,apiRoot,apiKey=None):
        self.s = sessions.BaseUrlSession(base_url=apiRoot)
        if apiKey is not None:
            self.s.headers.update({'Authorization': apiKey})
    
    def close(self):
        self.s.close()

    def get_session(self):
        return self.s

class CwmsApiSession:

    def __init__(self, api_root: str, api_key: str = None):
        if api_root is None:
            raise ValueError("CWMS API root URL cannot be None")
        self.__session = sessions.BaseUrlSession(base_url=api_root)
        if api_key is not None:
            self.__session.headers.update({'Authorization': api_key})

    def get_session(self):
        return self.__session


class _CwmsBase:

    def __init__(self, cwms_api_session: CwmsApiSession):
        if cwms_api_session is None:
            raise ValueError("CWMS API session information cannot be None")
        self._cwms_api_session = cwms_api_session

    def get_session(self):
        return self._cwms_api_session.get_session()