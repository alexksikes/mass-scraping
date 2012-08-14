#! /usr/bin/env python
# Author: Alex Ksikes (alex.ksikes@gmail.com)
# Using code from pycurl retriever-multi.py example

# TODO:
# - implement rotate ips
# - resolve the min conn min url choice
# - from file so no need to reread each time
# - with repeat all urls with problems are put at the end not sure it's the right approach
# - don't save empty files (the ones that errored)
# - when they are duplicate urls repository.py will break
# - in compress mode we had to close the directory
# >> alternatively we could catch ctrl-break and then close the repository

import hashlib
import os
import pycurl
import random
import repository

class Retriever:
    def __init__(self, conn, cookie_path=''):
        self.m = pycurl.CurlMulti()
        self.m.handles = []
        for i in range(conn):
            c = pycurl.Curl()
            c.fp = None
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.CONNECTTIMEOUT, 30)
            c.setopt(pycurl.TIMEOUT, 300)
            c.setopt(pycurl.NOSIGNAL, 1)
            #c.setopt(pycurl.USERAGENT, 'Googlebot/2.1 (+http://www.google.com/bot.html)')
            c.setopt(pycurl.USERAGENT, 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')

            if cookie_path:
                c.setopt(pycurl.COOKIEFILE, cookie_path)
                c.setopt(pycurl.COOKIEJAR, cookie_path)
            self.m.handles.append(c)

    def __init_urls(self, urls, shuffle=False, resume=[], no_duplicates=False, no_rename=False):
        if no_duplicates:
            urls = set(urls)
            urls = list(urls)
        if shuffle:
            random.shuffle(urls)
        if resume:
            resume = set(resume)
        self.queue = []
        for url in urls:
            if url in resume:
                continue
            if no_rename:
                filename = url.split('/')[-1]
            else:
                filename = hashlib.md5(url).hexdigest()
            self.queue.append((url, filename))
        self.num_urls = len(self.queue)

    def __read(self, f):
        # handle the repeat part as well
        urls = []
        for url in open(f):
            url = url.split('\t')[0].strip()
            if not url or url[0] == "#":
                continue
            urls.append(url)
        return urls

    def __run(self, out_folder='.', min_size=0, repeat=False, store=False, compress=False):
        self.store = store
        if store:
            self.repository = repository.Repository(root=out_folder, levels=store, compress=compress)
        if repeat:
            self.repeat = repeat
            self.repeat_list = {}
        freelist = self.m.handles[:]
        num_processed = 0
        while num_processed < self.num_urls:
            # If there is an url to process and a free curl object, add to multi stack
            while self.queue and freelist:
                url, filename = self.queue.pop(0)
                c = freelist.pop()
                #c.fp = repository.RepositoryFile(os.path.join(out_folder, filename), self.repository)
                c.fp = open(os.path.join(out_folder, filename), "wb")
                c.setopt(pycurl.URL, url)
                c.setopt(pycurl.WRITEDATA, c.fp)
                self.m.add_handle(c)
                # store some info
                c.filename = filename
                c.url = url
            # Run the internal curl state machine for the multi stack
            while 1:
                ret, num_handles = self.m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            # Check for curl objects which have terminated, and add them to the freelist
            while 1:
                num_q, ok_list, err_list = self.m.info_read()
                for c in ok_list:
                    c.fp.close()
                    c.fp = None
                    if store:
                        self.repository.add(os.path.join(out_folder, c.filename), remove=True)
                        # this makes the process slower but needed to keep integrity of the zip fies
                        self.repository.close()
                    self.m.remove_handle(c)
                    s = "\t".join([c.url,  c.filename, "SUCCESS", '', ''])
                    if repeat:
                        s += '\t' + str(self.__handle_repeat(c.url, c.filename, False))
                    print s
                    freelist.append(c)
                for c, errno, errmsg in err_list:
                    c.fp.close()
                    c.fp = None
                    if store:
                        self.repository.add(os.path.join(out_folder, c.filename), remove=True)
                        # this makes the process slower but needed to keep integrity of the zip fies
                        self.repository.close()
                    self.m.remove_handle(c)
                    s = "\t".join([c.url, c.filename, "FAILED", str(errno), errmsg])
                    if repeat:
                        s += '\t' + str(self.__handle_repeat(c.url, c.filename))
                    print s
                    freelist.append(c)
                num_processed = num_processed + len(ok_list) + len(err_list)
                if num_q == 0:
                    break
            # Currently no more I/O is pending, could do something in the meantime
            # (display a progress bar, etc.).
            # We just call select() to sleep until some more data is available.
            self.m.select(1.0)

    def __handle_repeat(self, url, filename, failed=True):
        count = self.repeat_list.get(url, 0)
        if failed:
            if count < self.repeat:
                self.queue.append((url, filename))
                self.num_urls += 1
            self.repeat_list[url] = count + 1
        return count

    def __clean_up(self):
        for c in self.m.handles:
            if c.fp is not None:
                c.fp.close()
                c.fp = None
            c.close()
        self.m.close()
        # make sure we close the repository in compress mode
        if self.store:
            self.repository.close()

    def dnl(self, urls, out_folder, shuffle, min_size, resume, repeat, store, compress, no_duplicates, no_rename):
        if isinstance(urls, str):
            urls = self.__read(urls)
        if isinstance(resume, str):
            resume = self.__read(resume)
        self.__init_urls(urls, shuffle, resume, no_duplicates, no_rename)
        self.__run(out_folder, min_size, repeat, store, compress)
        self.__clean_up()

def dnl(urls, conn=10, out_folder='.',
        shuffle=False, min_size=0, resume=[],
        repeat=False, store=False, compress=False,
        no_duplicates=False, no_rename=False, cookie_path=''):
    Retriever(conn, cookie_path=cookie_path).dnl(urls, out_folder, shuffle, min_size, resume, repeat, store, compress, no_duplicates, no_rename)

def usage():
    print "Usage:"
    print "    python retrieve.py [options] <list_of_urls>"
    print
    print "Description:"
    print "    Mass download a list of urls using various options."
    print
    print "    The list of urls is either a file (one line per  url)"
    print "    or comma separated from the command line"
    print "    or taken from stdin if set to '-'."
    print
    print "Options:"
    print "    -c, --conn <num_conn>              : number of concurrent connections"
    print "    -o, --out-folder <folder>          : folder to store the retrieved files"
    print "    -s, --shuffle                      : shuffle the list of urls first"
    print "    -m, --min-file-size <bytes>        : min file size before considered as an error"
    print "    -l, --sleep <num_failed> <sec>     : sleep for x seconds after num_failed failures"
    print "    -r, --resume <resume_file>         : resume download where it was left"
    print "    -p, --repeat <num_times>           : attempt to re-download the urls which failed"
    print "    -t, --store [num_levels]           : spread retrieved results in multiple directories"
    print "    -z, --compress                     : compress the result set"
    print "    -d, --no_duplicates                : remove duplicate urls"
    print "    -n, --no_rename                    : take the end of the url path as filename"
    print "    -i, --rotate <ip_1,...,ip_n> <sec> : rotate outgoing ip every x sec (not implemented)"
    print "    -k, --use_cookie <path>            : use existing cookie file"
    print "    -h, --help                         : this help message"
    print
    print "Email bugs/suggestions to Alex Ksikes (alex.ksikes@gmail.com)"

import sys, getopt, cStringIO
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:o:sm:l:r:p:t:zdnk:h",
                        ["conn=", "out-folder=", "shuffle=", "min-file-size=",
                         "sleep=", "resume=", "repeat=", "store=", "compress",
                         "no-duplicates", "no-rename", "use_cookie=", "help"])
    except getopt.GetoptError:
        usage(); sys.exit(2)

    conn, out_folder, min_size, resume = 10, '.', 0, []
    shuffle = sleep = repeat = store = compress = no_duplicates = no_rename = False
    cookie_path = ''
    for o, a in opts:
        if o  in ("-c", "--conn"):
            conn = int(a)
        elif o  in ("-o", "--out-folder"):
            out_folder = a
        elif o  in ("-s", "--shuffle"):
            shuffle = True
        elif o  in ("-m", "--min-file-size"):
            min_zize = int(a)
        elif o  in ("-l", "--sleep"):
            sleep = map(a.split())
        elif o  in ("-r", "--resume"):
            resume = a
        elif o  in ("-p", "--repeat"):
            repeat = int(a)
        elif o  in ("-t", "--store"):
            store = int(a)
        elif o  in ("-z", "--compress"):
            compress = True
        elif o  in ("-d", "--no-duplicates"):
            no_duplicates = True
        elif o  in ("-n", "--no-rename"):
            no_rename = True
        elif o  in ("-k", "--use_cookie"):
            cookie_path = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
    if len(args) < 1:
        usage()
    else:
        urls = sys.argv[-1]
        if urls == "-":
            urls = cStringIO.StringIO(sys.stdin.read())
        elif ',' in urls or urls.startswith('http://'):
            urls = urls.split(',')
        dnl(urls, conn=conn, out_folder=out_folder,
            shuffle=shuffle, min_size=min_size, resume=resume,
            repeat=repeat, store=store, compress=compress,
            no_duplicates=no_duplicates, no_rename=no_rename,
            cookie_path=cookie_path)

if __name__ == '__main__':
    main()
