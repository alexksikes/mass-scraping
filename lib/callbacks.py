from web import utils
from datetime import datetime
import re
import time

from html import strip_html
from html import unescape_html
from web.utils import strips, lstrips, rstrips

def clean(s):
    s = s.replace('\t', ' ')
    s = s.replace('\n', ' ')
    return re.sub('\s{2,}', ' ', s).strip()

def prepend(s, text='http://www.'): 
    return text + s

def booleanize(s):
    return s and '1' or '0'

def strptime(s, format):
    return datetime(*(time.strptime(s, format)[0:3]))
    
def mysql_date(s, formats):
    if not isinstance(formats, list or tuple):
        formats = [formats]
    
    s, _s = clean(s), ''
    for f in formats:
        try: 
            _s = strptime(s, f).strftime('%Y-%m-%d')
        except: 
            pass
        
    if _s:
        return _s
    return ''

def br2delimiter(s, delim='@#@'):
    s = re.sub('\s*(<br\s*/>|<br>)\s*', delim, s)
    return rstrips(s, '@#@')

def stripss(s, strs):
    for str in strs:
        s = strips(s, str)
    return s
