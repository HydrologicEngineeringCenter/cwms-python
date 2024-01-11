from importlib.metadata import PackageNotFoundError, version

from CWMSpy.cwms_loc import *
from CWMSpy.cwm,s_ts import *
from CWMSpy.utils import *


try:
    __version__ = version('CWMSpy')
except PackageNotFoundError:
    __version__ = 'version-unknown'