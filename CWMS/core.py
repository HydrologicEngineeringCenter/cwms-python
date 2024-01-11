import requests
from requests_toolbelt import sessions

from .cwms_ts import CwmsTsMixin
from .cwms_loc import CwmsLocMixin
#from .cwms_level import CwmsLevelMixin
#from .cwms_sec import CwmsSecMixin


class CWMS(CwmsLocMixin, CwmsTsMixin): #CwmsLevelMixin, CwmsSecMixin):
    
    def __init__(self, conn=None):
        self.conn = conn
        
    def connect(self,apiRoot,apiKey=None):
        self.s = sessions.BaseUrlSession(base_url=apiRoot)
        if apiKey is not None:
            self.s.headers.update({'Authorization': apiKey})
    
    def close(self):
        self.s.close()