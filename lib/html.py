# Author: Alex Ksikes 

import htmlentitydefs
import HTMLParser 
import re

class HTMLStripper(HTMLParser.HTMLParser):
    def __init__(self, convert_entity=True):
        self.reset()
        self.convert_entity = convert_entity
        self.fed = []
    
    def handle_data(self, d):
        self.fed.append(d)
    
    def handle_charref(self, d):
        if self.convert_entity:
            d = unescape_html('&#'+d+';')
        self.fed.append(d)
        
    def handle_entityref(self, d):
        if self.convert_entity:
            d = unescape_html('&'+d+';')
        self.fed.append(d)
        
    def get_fed_data(self):
        return ''.join(self.fed).strip()

def strip_html(html):
    s = HTMLStripper()
    s.feed(_unicode(html))
    return s.get_fed_data()

def unescape_html(value):
    return re.sub(r'&(#?)(\w+?);', _convert_entity, _unicode(value))

def _convert_entity(m):
    if m.group(1) == '#':
        try:
            if m.group(2)[0] == 'x':
                return unichr(int(m.group(2)[1:], 16))
            else:
                return unichr(int(m.group(2)))
        except ValueError:
            return '&#%s;' % m.group(2)
    try:
        return unichr(htmlentitydefs.name2codepoint[m.group(2)])
    except KeyError:
        return '&%s;' % m.group(2)

def _unicode(value):
    if isinstance(value, str):
        return value.decode('utf-8')
    assert isinstance(value, unicode)
    return value