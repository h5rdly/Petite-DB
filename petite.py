from __future__ import print_function  

ENC='utf-8'


#                                  Petite
#                                  ------
#
#                      Compressed key-value storage
#
#  
#  Petite takes data made of key and value strings, and stores them in an archive
#  (currently a zip file). It can then look up and modify entries from that
#  archive.
#
#  New entries are committed to the archive on adding, no need for sync() or  
#  commit().
#
#  Deleted entries are added to a second file, and are periodically (or 
#  explicitly) purged from the main archive.
#    
#  Purging is done the purge() or compact() functions, which rebuild the archive 
#  with only the up-to-date entries.
#
#
#  Usecases: 
#   Petite may come handy for k-v stores where:
#
#     % much data is generated and stored initially, and relatively little is 
#       changed per usage.
#
#     % the value data is considerable (entries are not brief) - zip doesn't
#       use solid compression, will shine given per-file substance. 
#
#
#  - Python dbm compatible (dictionary-like access and behavior, with persistence)
#
#  - Portable - Single file, no external libraries needed
#
#  - Tested on Python 2.7/3.6 under Windows 10


import zipfile, os
from io import open

zf=zipfile.ZipFile


# Decorators
# ----------   
'''Decorators used in the Zdbm class'''
   
def if_db_open(func):
    def wrapper(self, *args):
        def nope(*args):
            print ('No zdb file open')
            return
        return func(self, *args) if self._isopen else nope(*args)
    return wrapper

#add threading for update/compact, another dec. for testing


class Zdbm():
    '''
    The Zdbm class for accessing and manipulating a (currently) ZipFile in a 
    dictionary-like manner  '''
    
    
    def __init__(self, zdbfile=None, keys_as_property=False, COMPACT_AT_MB=100, 
                 verify_on_clear=False):
        
        '''
        keys_as_property - Get keys with .keys instead of .keys() when True. 
        
        COMPACT_AT_MB - upon closing, compact database if above that size in Mb. 
                        0/False to disable auto-compact.
        
        verify_on_clear - verify action when invoking .clear() method
        '''
        
        self._compact_threshold=COMPACT_AT_MB*1024*1024
        self._verify_on_clear= verify_on_clear
        self.mark_as_deleted=''   #Used by __delitem__ to mark deleted entries
        
        if keys_as_property:
           self.keys= self.keys()
            
        if zdbfile:
            self.open(zdbfile)
        
        
    # Properties
    # ----------
    
    '''  _isopen, _size   '''
    
    @property
    def _isopen(self):
        '''Used by the if_db_open decorator'''
        return True if self.zdbm else False
    
    @property                     
    @if_db_open
    def _size(self):
        return os.stat(self.zdbm.filename).st_size #use the zipfile info instead
    
    
    # User Facing Methods
    # -------------------
    
    ''' Include: open(zdbfile), close(), keys/keys(), updtae(dic), pop(key),
        compact()/purge(), clear()  '''
    
    def open(self, zdbfile):
        '''
        Opening an existing zdb file or creating a new one with desired name.
        
        A complimentary .zdb_o file is opened/created for holding the list of
        outadted (currently = deleted) keys.
        '''
        
        self.db=zdbfile
        
        try:
            self.zdbm=zf(self.db+'.zdb', 'a', allowZip64=True)
        except:
            print ('Issue opening database file')   #add errors to raise
        else:
            try:
                #Any deleted or modified entries are stored in a .zdb_o file
                self._outdated=zf(self.db+'.zdb_o', 'a', allowZip64=True)
            except:
                print ('Issue opening outdated records file')
            else:
                try:
                    #get the key list out of the (single) file inside the archive
                    self._outdated_keys = set(self._outdated.read('outdated keys').split('\n'))  
                except:
                    # Zipfile generates an empty file when one does not exist.
                    # In such case there's nothing to load, generating an empty set.
                    
                    self._outdated_keys=set()
                else:
                    # Removing deleted entries from NameToInfo. This is not
                    # persistent, aka not an on-disk modification
                    for removed_entry in self._outdated_keys:
                        try:
                            del self.zdbm.NameToInfo[removed_entry]
                        except KeyError:
                            pass
                
                self._outdated.close()

    
    @if_db_open
    def close(self):
        '''Optionally compact (rewrite) the database, then close it'''
        
        # Deleted entries as a single string for writing, empty if we compact
        deleted_as_string=''              
        
        if self._compact_threshold and self._size>= self._compact_threshold:    
             # Compact if threshold passed or explicitly asked 
             # NTS add threshold as relative to initial size 
            self.compact()
        
        # No compacting or done outside close(), appending current list of deleted entries
        deleted_as_string= '\n'.join(item for item in self._outdated_keys)
        print('deleted_as_string: ', deleted_as_string)
        # Flush all deleted entries to a single temp file. None if compacted. 
        with zf(self.db+'.zdb_o_', 'a', allowZip64=True) as self.new_outdated:
            # Flush to a new temp file
            self.new_outdated.writestr('outdated keys', deleted_as_string)
        
        # Remove old list, rename temp file back to the original name
        os.remove(self.db+'.zdb_o')
        print('removed old')
        os.rename(self.db+'.zdb_o_', self.db+'.zdb_o') 
         
        self.zdbm.close()
    
    
    @if_db_open    
    def keys(self):
        ''' NameToInfo() holds a dictionary of all up-to-date entries in the zip
        file. The zip may contain multiple files with the same name per folder, 
        but only (the latest) one will show up in NameToInfo()  
        '''
        
        return self.zdbm.NameToInfo.keys()         
    
    
    @if_db_open
    def update(self, dic):
        '''Update dbm from a dictionary'''
        
        if not type(dic) is dict:
            print('Pass a dictionary to update the dbm with')
            return
        
        for key in dic:
            # Using the defined __setitem__ to update dbm
            self[key]=dic[key]
        
    
    @if_db_open    
    def pop(self, key):
        
        # Using __delitem__ to update dbm
        del self[key]
    
        
    #@if_db_open
    def compact(self):
    
        if not self.zdbm:
            print ('No zdb file open')
            return
            
        # buffered writing ( a chunk of files every time) should be faster, look
        # to implement eventually (perhaps mmap?)
        with zf(self.db+'_', 'a', allowZip64=True) as compacted_db:
            for key in self.zdbm.NameToInfo:
                compacted_db.writestr(key, self[key])
        
        print('compacted file created')
        
        # The close() method will clear out the outdated keys file
        self._outdated_keys=set()
        print('self._outdated_keys: ', self._outdated_keys)
        self.zdbm.close()
        
        # Delete old db file, rename new back to the original name                  
        os.remove(self.db+ '.zdb')        #self.zdbm=zf(self.db+'.zdb'  
        os.rename(self.db+'_', self.db+ '.zdb')
        #os.remove(self.db+ '_') 
        #os.remove(self.db+'.zdb_o')   #A new one will be created
        
        self.open(self.db)
        
    
    def purge(self):
        '''Alias to compact()'''
        self.compact()
    
    
    @if_db_open
    def clear(self):
        if self._verify_on_clear:
            pass   #add input Y/N, return if not Y
        
        # Close and delete
        self.zdbm.close()
        os.remove(self.db)
        os.remove(self.db+'.zdb_o')
        
        # Open new file with same name
        self.open(self.db)
        
        
    # Internal Methods
    # ----------------
    
    ''' Include: __getitem__() , __setitem__() , __delitem__() , __enter__() , 
         __exit__() , __iter__() '''
    
    @if_db_open
    def __getitem__(self, key):         # validate existence of archive and item
        try:
            value = self.zdbm.read(key)
        except KeyError:
            print ('Non Existent key')
        else:
            if value:
                return value
            else:
                # Empty values mark deleted values that have not yet been purged
                print ('Non Existent key')
                
                
    @if_db_open
    def __setitem__(self, key, value):         #validate key, value are strings
        '''If a key already exists in the db, the previous key/value (=filename/content) 
        is not removed, but NameToInfo stores and shows only the latest entry.
        Outdated entries are purged by compact()'''
        
        if key in self._outdated_keys and value:
            '''In case a key was removed and a same named key was re-added
            before purging. 
            '''
            self._outdated_keys.remove(key) 
            
        try:
            self.zdbm.writestr(key, value)    
        except:  #this isn't extensive, pick another way
            print('Warning - Non string value passed. Saved as string')
            self.zdbm.writestr(str(key), str(value))
        
    
    @if_db_open
    def __delitem__(self, key):
        '''Deleted keys are also updated with an empty value as an added measure, 
        overwriting any previous versions in NameToInfo() '''
        
        #not using keys() since curently open to being either a method or property
        if not key in self.zdbm.NameToInfo.keys():  
            print ('Key not in dbm')
            return
         
        # Deleted key updated with an empty value for added precaution    
        print ('Key set to be deleted, will be permanently removed upon purge()')
        print ('Ignore the warning \n')
        self.__setitem__(key, self.mark_as_deleted) 
        
        
        # Adding key to the removed list and removing from the key index
        self._outdated_keys.add(key) 
        del self.zdbm.NameToInfo[key]

    def __enter__(self):
        return self
        
        
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            print(exc_type, exc_value, traceback)
        
        self.close()


    @if_db_open
    def __iter__(self):
        '''NameToInfo holds a dictionary with (latest version of) filenames as 
        keys and their ZipInfo objects as values'''
        
        for key in self.zdbm.NameToInfo:
            yield key
    
    '''
    def __repr__(self):
        
        if not self._isopen:
            return str({})
        
        return str({key: self[key] for key in self.zdbm.NameToInfo.keys()})'''