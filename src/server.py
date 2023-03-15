import os
import plyvel
import json
import hashlib
import random
import xattr
import tempfile

print("hello", os.environ['TYPE'], os.getpid())

# helper fns
def resp(start_response, code, headers=[('Content-type', 'text/plain')], body=b''):
    start_response(code, headers)
    return [body]

# *** Master Server ***

if os.environ['TYPE'] == 'master':
    # check on volume servers
    volumes = os.environ['VOLUMES'].split(',')

    for v in volumes:
        print(v)

    db = plyvel.DB(os.environ['DB'], create_if_missing=True)

def master(env, sr):
    key = env['REQUEST_URI']

    if env['REQUEST_METHOD'] == 'POST':
         # POST is called by the volume servers to write to the database
        flen = int(env.get('CONTENT_LENGTH', 0))

        if flen > 0:
            db.put(key.encode('utf-8'), env['wsgi.input'].read(flen), sync=True)
        else:
            db.delete(key.encode('utf-8'))
        return resp(sr, '200 OK')

    metakey = db.get(key.encode('utf-8'))
    if metakey is None:
        if env['REQUEST_METHOD'] == 'PUT':
            # handle putting key
            # TODO: make volume selection intelligent
            volume = random.choice(volumes)

            # save volume to database
            meta = {"volume": volume}
            # remember which volue the key is on
            db.put(key.encode('utf-8'), json.dumps(meta).encode('utf-8')) 
        else:
            # this key doesn't exist and we aren't trying to create it
            return resp(sr, '404 Not Found')
    else:
        # key found and we are trying to put it
        if env['REQUEST_METHOD'] == 'PUT':
            # key already exists, can't overwrite (update)
            return resp(sr, '409 Conflict')

        meta = json.loads(metakey.decode('utf-8')) # know which volume
        volume = meta['volume']

    headers = [('Location', 'http://%s%s' % (volume, key))]

    return resp(sr, '307 Temporary Redirect', headers)


# *** Volume Server ***

class FileCache(object):
    def __init__(self, basedir):
        self.basedir = os.path.realpath(basedir)
        self.tmpdir = os.path.join(self.basedir, "tmp")
        os.makedirs(self.tmpdir, exist_ok=True)
        print("FileCache in %s" % basedir)

    def k2p(self, key, mkdir_ok=False):
        key = hashlib.md5(key).hexdigest()

        # 2 layers deep dir structure
        path = self.basedir+"/"+key[0:2]+"/"+key[0:4]
        if not os.path.isdir(path) and mkdir_ok:
            os.makedirs(path, exist_ok=True)

        return os.path.join(path, key)

    def exists(self, key):
        return os.path.isfile(self.k2p(key))

    def delete(self, key):
        os.unlink(self.k2p(key))

    def get(self, key):
        return open(self.k2p(key), 'rb')

    def put(self, key, stream):
        with tempfile.NamedTemporaryFile(dir=self.tmpdir, delete=False) as f:
            # TODO: write in chunks, don't load whole file into memory once
            f.write(stream.read())

        # save the real name in xattr in case we rebuild it (before hashed)
        xattr.setxattr(f.name, 'user.key', key)
        os.rename(f.name, self.k2p(key, True)) # change to hashed name

if os.environ['TYPE'] == 'volume':
    host = os.environ['HOST'] + ':' + os.environ['PORT']
    print(host)

    # create the filecache
    fc = FileCache(os.environ['VOLUME'])


def volume(env, sr):
    key = env['REQUEST_URI'].encode('utf-8')

    if env['REQUEST_METHOD'] == 'PUT':
        if fc.exists(key):
            # can't overwrite
            return resp(sr, '409 Conflict')

        # otherwise, put that file into fs
        flen = int(env.get('CONTENT_LENGTH', 0))
        if flen > 0:
            fc.put(key, env['wsgi.input'])
            return resp(sr, '201 Created')
        else:
            return resp(sr, '411 Length Required')

    if not fc.exists(key):
        # key not in the FileCache, 404
        return resp(sr, '404 Not Found')

    if env['REQUEST_METHOD'] == 'GET':
        # TODO: in chunks
        return resp(sr, '200 OK', body=fc.get(key).read())

    if env['REQUEST_METHOD'] == 'DELETE':
        fc.delete(key)
        return resp(sr, '200 OK')
