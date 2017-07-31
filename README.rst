Petite-LZMA
===========

I'm experimenting with utilizing Python's LZMA library for storing key-values in a solid-block compression archive.

Current path/guidelines:
------------------------

- Keys and Values folders inside a generated DB folder
- Values are stored in blocks - a bunch of enumerated .xz files of preset size.
- Keys can be saved in either a single file or in blocks as well. 
- Initially, it'll be in-memory - keys will be extracted into a set or perhaps OrderedDict.
- Any "work" done will require updating the relevant value blocks, as well as the key file/relevant blocks.
- Scheme to connect keys to values - mark by block number and position inside block.
- backports.lzma should work in a similar fashion to lzma on Python 2.7
- Figure out some incremental scheme for new keys/values, supported via  LZMACompressor
