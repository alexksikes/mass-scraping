# Author: Alex Ksikes (alex.ksikes@gmail.com)

import extract 
import MySQLdb 
import os 
import re

def get_cursor_from_env():
    user, db = os.getenv('USER'), os.getenv('DB')
    passwd = raw_input('Password (%s@localhost for db "%s"): ' % (user, db))
    db = MySQLdb.connect(host='localhost', user=user, passwd=passwd, db=db)
    return db.cursor()

def create_table(c, config, tbl_name, columns=[], drop=False):
    if drop:
        sql = 'drop table if exists %s' % tbl_name
        print sql
        c.execute(sql)
    sql = 'create table %s (\n' % tbl_name
    
    if config.has_key('sql'):
        sql += '\t%s,\n' % config['sql']
        
    for f in config['fields']:
        if columns and f not in columns:
            continue
        sql += '\t%s\t%s,\n' % (f, config[f]['sql'])
    sql = sql[:-2] + '\n) charset utf8'
    print sql
    c.execute(sql)
    
def populate(c, tab_file, tbl_name, columns=[], duplicates='ignore', ignore_lines=1):
    if columns:
        columns = '(%s)' % ', '.join(columns)
    else:
        columns = ''
    if not ignore_lines:
        ignore_lines = ''
    else:
        ignore_lines = 'ignore %s lines' % ignore_lines
    
    sql = 'load data local infile "%s" %s into table %s %s %s' % \
        (tab_file, duplicates, tbl_name, ignore_lines, columns)
    print sql
    c.execute(sql)
    
def populate_split(c, tab_file, tbl_name, columns=[], num_lines=1000000, duplicates='ignore', ignore_lines=1):
    for i, f in enumerate(get_split_file(tab_file, num_lines)):
        if i == 0:
            ignore_lines = 1
        else:
            ignore_lines = 0
        populate(f, tbl_name, columns, duplicates, ignore_lines)

def ls_split_file(dir):
    for f in sorted(os.listdir(dir)):
        if re.match('x\d\d', f):
            yield os.path.join(dir, f)

def get_split_file(path, num_lines):
    print 'Splitting the the tab files ...'
    cwd = os.path.dirname(path)
    os.chdir(cwd)
    os.system('split -dl %s %s' % (num_lines, path))
    
    for f in ls_split_file(cwd):
        yield f
        os.system('rm %s' % f)
        
def read_config_file(config_file):
    config = extract.parse_config(config_file, compile_regex=False)
    config['fields'].insert(0, 'filename')
    config['filename'] = {'sql' : 'varchar(32)'}
    return config
    
def run(config_file, tab_file, tbl_name, columns=[], drop=False, split=False):
    config = read_config_file(config_file)
    cursor = get_cursor_from_env()
    
    create_table(cursor, config, tbl_name, columns, drop)
    if split:
        populate_split(cursor, tab_file, tbl_name, columns, num_lines=split)
    else:
        populate(cursor, tab_file, tbl_name, columns)
                
def usage():
    print 'Usage:' 
    print '    python populate.py config_file tab_file tbl_name'
    print
    print 'Description:' 
    print '    Read the config file and the tab file'
    print '    and load the data into a mysql table.'
    print
    print 'Options:' 
    print '    -d, --drop              : drop table if it exists'
    print '    -c, --columns           : insert these columns only'
    print '    -s, --split <num>       : split the tab file into chunks of num rows each'
    print
    print 'Email bugs/suggestions to Alex Ksikes (alex.ksikes@gmail.com)' 

import sys, getopt
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:ds:h', 
            ['columns=', 'drop', 'split=', 'help'])
    except getopt.GetoptError:
        usage(); sys.exit(2)
    
    columns, drop, split = [], False, False
    for o, a in opts:
        if o in ('-c', '--columns'):
            columns = a.split()
        elif o in ('-d', '--drop'):
            drop = True
        elif o in ('-s', '--split'):
            split = int(a)
        elif o in ('-h', '--help'):
            usage(); sys.exit()
        
    if len(args) < 2:
        usage()
    else:
        run(args[0], args[1], args[2], columns, drop, split)
        
if __name__ == '__main__':
    main()
