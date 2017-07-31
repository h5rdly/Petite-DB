from __future__ import print_function  

ENC='utf-8'


#                              Petite - MkII
#                              -------------
#
#                        Compressed key-value storage
#
#  
#  Petite stores key-value data in solid-block compressed LZMA archives.
#
#  Solid-block compression means data is compressed into chunks of preset size.
#  This allows selectively unpacking relevant data, while maintaining the 
#  database's footprint on the disk.
#
#  It also allows scaling in that not all (or any) keys need to be pre-extracted
#  into memory, at the price of lookup speed.
#
#  Currently used to allow pure Python apps persistent storage of k-v data, while 
#  keeping the db size honest.
#


import os
from io import open
try:
    import lzma
except:
    try:
        from backports import lzma     # pip install backports.lzma for Python 2.7
    except:
        print ('No lzma library found')
        return


class Lzdb():
    '''
    The Lzdb classs holds the interface to operate, search and manipulate
    solid-block compressed LZMA files containing key-value data   '''
    
    def __init__(self, ):
        
        pass
    
    
    # Properties
    # ----------
    
    
    # User Facing Methods
    # -------------------
    
    ''' Include: open(xzdbfile)  '''
    
    def open(self, xzdbfile):
        '''
        Opening an existing xzdb file or creating a new one with desired name.
        
        '''
        
        self.db=xzdbfile
        
        try:
            self.xzdb=zf(self.db+'.zdb', 'a', allowZip64=True)
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
                            del self.zdbm.NameToInfo[removed_entry.decode(ENC)]
                        except KeyError:
                            pass
                finally:
                    # Zdbm instance was opened sucessfully, making keys() a
                    # property
                    
                    if self.keys_as_property:
                        try:
                            self.keys= self.keys() 
                        except:
                            # When reopening a db file in the same session, 
                            # keys was already made a property
                            pass  
                    
                self._outdated.close()
    
    
    # Internal Methods
    # ----------------
    
    ''' Include: __getitem__() , __setitem__()  '''
    
    def __getitem__(self, key):         # validate existence of archive and item
        try:
            value = self.zdbm.read(key.encode(ENC))
        except KeyError:
            print ('Non Existent key')
        else:
            if value:
                return value.decode(ENC)
            else:
                # Empty values mark deleted values that have not yet been purged
                print ('Non Existent key')
                
                
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
            self.zdbm.writestr(key.encode(ENC), value.encode(ENC))    
        except:  #this isn't extensive, pick another way
            print('Warning - Non string value passed. Saved as string')
            self.zdbm.writestr(str(key).encode(ENC), str(value).encode(ENC))
