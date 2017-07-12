- A simple key-value storage
- Single interface across platforms
- Strives to reduce hard-disk footprint*.


Python offers persistent key-value storage (saving dictionaries to disk) 
via the [link]dbm module. However, I was unsatisfied with dbm - 

- The API to all backends but the deprecated Berkeley DB is not complete - 
  no iteration, no clear() method. 
- While Berkeley DB offered a complete API and was available on both Windows
  and Linux, it was deprecated on Python 3.
- Thus, as of Python 3, those backends exist only on some Unix systems, and need
  to be handled differently than dumbdbm.
- The only cross-platform dbm option is Python's own dumbdbm, which is self 
  proclaimedly lame.


Solutions I've tried:

- LMDB - cross platform, has a Python package, used by SQLite, data scientists and many more. This is a well acclaimed k-v solution that uses transactions, thus complicating the approach somewhat. Most importantly (to my needs), [link]it does not support compression.

- semidbm - a much better python dbm implementation. while fast and pure python (pip install always works, no need for the 3Gb Visual 2014 monster thingy on Windows), it too offers no built-in compression.


What does Petite offer?

- A persistent dictionary interface:
  [example goes here]
  
- no need for sync() / commit()

- Small and portable - A dime over 300 lines total.


Drawbacks:
 
- * Zip does not offer solid-block compression, so not much space can be saved. 
- While adding entries is quick, compacting entails rebuilding the database, akin to semidbm. 
 
 
To-do:

- Find if it's somehow possible to regenerate the database/zipfile without de/recompression, perhaps using ZipFile's compress_size, header_offset

- The small footprint goal is not currently achieved. Therefore, add support for solid-block compression, currently looking at LZMA or 7z.

- benchmark against other Python solutions.  

Public Domain.
