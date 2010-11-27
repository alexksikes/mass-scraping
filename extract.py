# Author: Alex Ksikes (alex.ksikes@gmail.com)

import codecs
import glob
import os
import re
import sys
import traceback
import zipfile

from lib.callbacks import *

class PatternExtractor(object):
    def __init__(self, config_file, fields=[], fout=None, exclude=[]):
        self.fields = fields
        self.exclude = exclude
        self.fout = fout
        self.config = parse_config(config_file, include=fields, exclude=exclude)
        for k in ['fields', 'regex_mode', 'regex_flag']:
            setattr(self, k, self.config[k])
        self.regex_item = self.config.get('regex_item')
            
    def extract(self, html):
        if self.regex_mode == 'global':
            self.results = self._extract_global(html)
        else:
            self.results = self._extract_inline(html)
            
    def _extract_inline(self, html):
        results = []
        for field in self.fields:
            r = self._extract_inline_one(field, html)
            results.append(r)
        return results

    def _extract_inline_one(self, field, html):
        # apply the regex for the particular field
        c = self.config[field]
        regex = c['regex']
        r = regex.search(html)
        r = r and r.group(1) or ''
            
        # apply each callback to the result
        if not r: return r
        for cbk in c['callback']:
            r = cbk(r)
        return r
        
    def _extract_global(self, html):
        regex = self.config['regex_global']
        matches = regex.findall(html, self.regex_flag)
        
        # see if this is the desired behvior
        if not matches: 
            return []
        
        if isinstance(matches[0], tuple):
            matches = zip(*matches)
        else:
            matches = [matches]
        
        results = []
        for field in self.fields:
            c = self.config[field]
            
            # extract all fields in a group in order
            if c.has_key('regex_group'):
                r = matches[c['regex_group']]
                for cbk in c['callback']:
                    r = map(cbk, r)
                    
            # the other fields are just repeated
            else:
                r = self._extract_inline_one(field, html)
                r = [r]*len(matches[0])
            
            results.append(r)
        return results
    
    def header_print(self, id_name='id'):
        id_name = id_name and (id_name + '\t') or ''
        self._print_out(id_name + '\t'.join(self.fields))
        
    def row_print(self, id=0):
        '''Print the results in a table for easy mysql insertion.'''
        assert hasattr(self, 'results')
        
        # not sure we want this behavior!
        if not self.results: return
        
        id = id and (id + '\t') or ''
        
        s = ''
        if self.regex_mode == 'global':
            for result in zip(*self.results):
                result = [r or '\N' for r in result]
                s += id + '\t'.join(result) + '\n'
            s = s[:-1]
        else:
            results = [r or '\N' for r in self.results]
            s += id + '\t'.join(results)
        self._print_out(s)
        
    def pprint(self):
        '''Print the results in an easier to read format.'''
        assert hasattr(self, 'results')
        
        # in global mode a table is easier to read
        if self.regex_mode == 'global':
            h = '-*-'.join(self.fields)
            print '***-%s-***' % h
            self.row_print(id='')
            return

        # otherwise print each field below each other
        s = ''
        for field, result in zip(self.fields, self.results):
            s += '***%s%s%s***\n' % ('-'*15, field, '-'*(25 - len(field)))
            if isinstance(result, list):
                s += '\n'.join(result) + '\n'
            else:
                s += result + '\n'
        print s[:-1].encode('utf-8')
        
    def _print_out(self, s):
        s = s.encode('utf-8')
        if self.fout:
            self.fout.write(s + '\n')
        else:
            print s

def parse_config(config_file, compile_regex=True, include=[], exclude=[]):
    # the default values
    callback = [unescape_html, clean]
    regex_flag = re.I|re.S
    regex_mode = 'inline'
    callback_post = [clean]
    config = {}
    
    # include or exlude chosen fields
    cf = open(config_file).read()
    fields = re.findall('^@(\w+)\s*=\s*', cf, re.M)
    if include:
        fields = [f for f in fields if f in include]
    if exclude:
        fields = [f for f in fields if f not in exclude]
    config['fields'] = fields
    
    # read the other values in config
    p = re.compile('^(@|$)', re.M)
    exec(p.sub('', cf), globals(), config)
    
    # parse global values
    for k in ['regex_mode', 'regex_flag', 'callback', 'callback_post']:
        config[k] = config.get(k, locals()[k])
    
    # compile regex if needed
    if compile_regex:
        for c in [config[f] for f in fields]:
            if not c.has_key('regex_group'):
                c['regex'] = re.compile(c['regex'], c.get('regex_flag', config['regex_flag']))

    # for global regex mode
    if config['regex_mode'] == 'global':
        config['regex_global'] = re.compile(config['regex_global'], config['regex_flag'])

    # assign callbacks with their default values
    for c in [config[f] for f in fields]:
        cbk = c.get('callback', [])
        if not isinstance(cbk, list):
            cbk = [cbk]
        c['callback'] = callback + cbk + callback_post
    
    # when we have multiple items in a file
    if config.get('regex_item'):
        config['regex_item'] = re.compile(config['regex_item'], re.M|re.S)

    return config

def dir_iter(d, enc='utf-8'):
    for dirpath, dirnames, filenames in os.walk(d):
        if dirnames: continue
        for f in sorted(filenames):
            path = os.path.join(dirpath, f)
            yield os.path.basename(f), open(path).read().decode(enc)
                    
def repository_iter(d, enc='utf-8'):
    for a, zf in dir_iter(d):
        zf = zipfile.ZipFile(zf, 'r')
        for f in zf.namelist():
            yield f, zf.read(f).decode(enc)

def is_repository(repo):
    for d in os.listdir(repo):
        if not zipfile.is_zipfile(os.path.join(repo, d)):
            return False
    return len(os.listdir(repo)) == 256

def extract(cf, fi, fields, enc='utf-8', pprint=False, exclude=[], out_dir='.'):
    def make_parsers(cf):
        parsers = []
        for c in cf:
            if not pprint:
                fi = rstrips(os.path.basename(c), '.conf') + '.tbl'
                fi = os.path.join(out_dir, fi)
                fout = open(fi, 'w')
            else:
                fout = None
            parsers.append(PatternExtractor(c, fields, fout=fout, exclude=exclude))
        return parsers
    
    def _extract(parser, html):
        try:
            parser.extract(html)
        except:
            sys.stderr.write('\nError on file %s:' % f)
            traceback.print_exc()
        if pprint:
            parser.pprint()
        else:
            parser.row_print(id=f)

    def get_html_chunks(html, regex):
        return regex.findall(html)
    
    if isinstance(cf, list):
        parsers = make_parsers(cf)
    else:
        parsers = [PatternExtractor(cf, fields, exclude=exclude)]
    
    if not pprint:
        for parser in parsers:
            parser.header_print(id_name='filename')
    
    if os.path.isfile(fi):
        ls = [(os.path.basename(fi), open(fi).read().decode(enc))]
    elif is_repository(fi):
        ls = repository_iter(fi, enc)
    else:
        ls = dir_iter(fi, enc)
    
    for f, html in ls:
        for parser in parsers:
            if parser.regex_item:
                for chunk in get_html_chunks(html, parser.regex_item):
                    _extract(parser, chunk)
            else:
                _extract(parser, html)
                    
def usage():
    print 'Usage:' 
    print '    python extract.py -c conf_file(s) <file or directory or repository>'
    print 
    print 'Description:' 
    print '    Read the config file and output a tab delimited file'
    print '    each column being the extract pattern in each file.'
    print
    print '    If multiple conf file are provided the program outputs' 
    print '    a file name_of_conf_file.tbl for each conf file.'
    print
    print '    Note that the output encoding is always UTF-8.'
    print '    When only a file is specified pprint is automatically on.'
    print '    When extracting multiple files the filename is also printed.'
    print '    (when pprint is on the filename is never shown)'
    print 
    print 'Options:' 
    print '    -c, --conf              : path to conf file(s) (mandatory)'
    print '    -p, --pprint            : pretty print to stdout'
    print '    -o, --out_dir           : output directory for tbl files (default cwd)'
    print '    -f, --fields            : only extract specific fields'
    print '    -x, --exclude           : exclude specific fields'
    print '    -e, --encoding enc      : input encoding (default is utf-8)'
    print
    print 'Email bugs/suggestions to Alex Ksikes (alex.ksikes@gmail.com)' 

import sys, getopt
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:f:x:o:e:hp', 
            ['conf=', 'fields=', 'exclude=', 'out_dir', 'encoding=', 'help', 'pprint'])
    except getopt.GetoptError:
        usage(); sys.exit(2)
    
    encoding, fields, pprint, exclude, out_dir = 'utf-8', [], False, [], '.'
    for o, a in opts:
        if o in ('-c', '--conf'):
            cf = glob.glob(a)
            if not cf:
                cf = a.split()
        elif o in ('-p', '--pprint'):
            pprint = True  
        elif o in ('-f', '--fields'):
            fields = a.split()  
        elif o in ('-o', '--out_dir'):
            out_dir = a  
        elif o in ('-x', '--exclude'):
            exclude = a.split()  
        elif o in ('-e', '--encoding'):
            encoding = a  
        elif o in ('-h', '--help'):
            usage(); sys.exit()
    
    if len(args) < 1:
        usage()
    else:
        cf = len(cf) == 1 and cf[0] or cf
        pprint = os.path.isfile(args[0]) or pprint
        extract(cf, args[0], fields, encoding, pprint, exclude, out_dir)
        
if __name__ == '__main__':
    main()
