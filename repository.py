# Author: Alex Ksikes (alex.ksikes@gmail.com)

import cStringIO
import os
import re
import shutil
import zipfile

class Repository:
    def __init__(self, root='.', levels=1, width=2, 
                 strip=False, compress=False, force_md5=False):
        self.root = root
        self.levels = levels
        self.width= width
        self.strip = strip
        self.compress = compress
        if compress:
            self.zips = {}
        self.dirs = []
    
    def add(self, f_path, remove=False):
        f = os.path.basename(f_path)
        leaf = self.get_leaf(f)
        if self.strip:
            f = self.strip_name(f)
        self.make_dir(leaf)
        
        if self.compress:
            self.make_zip(leaf)
            z = self.get_zip(leaf)
            # the preferred behavior is to overwrite (same as copy)
            try:
                z.getinfo(f)
                z = self.zip_delete(z, leaf, f)
            except KeyError:
                pass
            z.write(f_path, f, zipfile.ZIP_DEFLATED)
        else:
            shutil.copy2(f_path, os.path.join(os.path.join(self.root, leaf), f))
        if remove:
            os.remove(f_path)
    
    def get_file(self, f):
        leaf = self.get_leaf(f)
        if self.strip:
            f = self.strip_name(f)
        
        if self.compress:
            fi = cStringIO.StringIO(self.get_zip(leaf).read(f))
        else:
            fi = open(os.path.sep.join([self.root, leaf, f]))
        return fi
    
    def copy_file(self, f, out_dir):
        fi = self.get_file(f)
        shutil.copyfileobj(fi, open(os.path.join(out_dir, f), 'w'))
        
    def get_leaf(self, f):
        return os.path.sep.join(split_count(f, self.width)[:self.levels])
        
    def make_dir(self, d):
        if self.compress:
            d = d[:-1*self.width]
        d_path = os.path.join(self.root, d)
        if d not in self.dirs and not os.path.isdir(d_path):
            os.makedirs(d_path, 0744)
            self.dirs.append(d)
    
    def make_zip(self, d):
        if not self.zips.has_key(d):
            z_path = os.path.join(self.root, d) + '.zip'
            if os.path.isfile(z_path):
                mode = 'a'
            else:
                mode = 'w'
            self.zips[d] = zipfile.ZipFile(z_path, mode)
            
    def get_zip(self, d):
        if not self.zips.has_key(d):
            self.make_zip(d)
        return self.zips[d]
    
    def zip_delete(self, z, leaf, f):
        # this uglyness is due to the fact that the zipfile
        # module does not support delete
        # for consistency we have to do what follows
        z.close()
        del(self.zips[leaf])
        zip_delete(z.filename, f)
        self.make_zip(leaf)
        return self.get_zip(leaf)
        
    def strip_name(self, f):
        stripped = ''.join(split_count(f, self.width)[:self.levels])
        return re.sub('^' + stripped, '', f)
        
    def unstrip_name(self, full_path, f):
        return ''.join(full_path.split(os.path.sep)[1:]) + f
        
    def close(self):
        if self.compress:
            for z in self.zips.values():
                z.close()
            self.zips = {}
    
    def get_full_filename(self, f):
        leaf = self.get_leaf(f)
        if self.compress:
            return self.get_zip(leaf)
        return os.path.sep.join([self.root, leaf, f])
        
    def __getitem__(self, f):
        leaf = self.get_leaf(f)
        if self.compress:
            return self.get_zip(leaf)
        return open(os.path.sep.join([self.root, leaf, f]), 'r')
    
    def __delete__(self, f):
        leaf = self.get_leaf(f)
        if self.compress:
            zip_delete(self.get_zip(leaf).filename, f)
        else:
            os.remove(os.path.join(self.root, f))
    
    def __iter__(self):
        for dirpath, dirnames, filenames in os.walk(self.root):
            if dirnames: continue
            for f in sorted(filenames):
                if self.strip:
                    f = self.unstrip_name(dirpath, f)
                yield self.get_full_filename(f)
                    
class RepositoryFile(file):
    def __init__(self, f_path, repository):
        self.f_path = f_path
        self.repository = repository
        
    def write(self):
        self.repository.add(self.f_path)
        
    def close(self):
        pass

def split_count(s, count):
    return [''.join(x) for x in zip(*[list(s[z::count]) for z in range(count)])]

from subprocess import Popen, PIPE
# zipfile does no support file removal
def zip_delete(z_path, f):
    #os.system('zip -d %s %s' % (z_path, f))
    p = Popen('zip -d %s %s' % (z_path, f), shell=True, stdout=PIPE, stderr=PIPE)
    sts = os.waitpid(p.pid, 0)
    
def spread(in_folder, out_folder, levels, 
           width, strip, compress, force_md5):
    r = Repository(out_folder, levels, width, strip, compress, force_md5)
    for f in os.listdir(in_folder):
        r.add(os.path.join(in_folder, f))
    r.close()

def usage():
    print "Usage: "
    print "    python repository.py [options] <source_directory>"
    print
    print "Description:" 
    print "    Use this tool to spread files named by an md5 into multiple subdirectories"
    print "    058130813613f0cbe6d2fdd03fdaadf5 go to 05/8130813613f0cbe6d2fdd03fdaadf5"
    print
    print "Options:" 
    print "    -o, --out-folder <folder> : where the store is located (default cwd)"
    print "    -l, --levels <num_levels> : how many sub directories (default 1)"
    print "    -w, --width <num_char>    : num of char of a subfolder name (default 2)"
    print "    -s, --strip               : do not repeat name found in sub dir"
    print "    -z, --compress            : compress the leaf subfolders on the fly"
    print "    -m, --force-md5           : if the file do not have an md5 filename"
    print "    -h, --help                : this help message"
    print
    print "Email bugs/suggestions to Alex Ksikes (alex.ksikes@gmail.com)" 
    
import sys, getopt
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "o:l:w:szmh", 
                        ["out-folder=", "levels=", "width", "strip",
                         "compress", "force-md5", "help"])
    except getopt.GetoptError:
        usage(); sys.exit(2)
    
    out_folder, levels, width = '.', 1, 2
    strip = compress = force_md5 = False
    for o, a in opts:
        if o  in ("-o", "--out-folder"):
            out_folder = a
        elif o  in ("-l", "--levels"):
            levels = int(a)
        elif o  in ("-w", "--width"):
            width = int(a)
        elif o  in ("-s", "--strip"):
            strip = True
        elif o  in ("-z", "--compress"):
            compress = True
        elif o  in ("-m", "--force-md5"):
            force_md5 = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
    if len(args) < 1:
        usage()
    else:
        spread(sys.argv[-1], out_folder, levels, width, strip, compress, force_md5)
              
if __name__ == '__main__':
    main()
