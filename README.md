# weikv

A non-proxying distributed key value store. Optimized for reading files between 1 MB and 1 GB.


### API
- GET /key
  * Supports range requests.
  * 302 redirect to volume server.
- {PUT, DELETE} /key
  * Blocks. 200 = written, anything else = nothing happend.

