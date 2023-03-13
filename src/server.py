import os
import plyvel
import json
import socket
import hashlib
import random

print("hello", os.environ['TYPE'], os.getpid())

# *** Master Server ***

if os.environ['TYPE'] == 'master':
    # check on volume servers
    volumes = os.environ['VOLUMES'].split(',')

    for v in volumes:
        print(v)

    db = plyvel.DB(os.environ['DB'], create_if_missing=True)

def master(env, start_response):
    key = env['REQUEST_URI']
    metakey = db.get(key.encode('utf-8'))

    # debugging
    print(metakey)
    print("|")

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
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [b'Key Not Found']
    else:
        # key found and we are trying to put it
        meta = json.loads(metakey.decode('utf-8')) # know which volume

    print(meta)
    volume = meta['volume']
    headers = [('Location', 'http://%s%s' % (volume, key))]
    start_response('307 Temporary Redirect', headers)
    return [b'']


# *** Volume Server ***

class FileCache(object):
    def __init__(self, basedir):
        self.basedir = os.path.realpath(basedir)
        os.makedirs(self.basedir, exist_ok=True)
        print("FileCache in %s" % basedir)
    def k2p(self, key, mkdir_ok=False):
        # must be MD5 hashing
        assert len(key) == 32

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
        return open(self.k2p(key), 'rb').read()

    def put(self, key, value):
        with open(self.k2p(key, mkdir_ok=True), 'wb') as f:
            f.write(value)

if os.environ['TYPE'] == 'volume':
    host = socket.gethostname()

    # create the filecache
    fc = FileCache(os.environ['VOLUME'])


def volume(env, start_response):
    key = env['REQUEST_URI'].encode('utf-8')
    hkey = hashlib.md5(key).hexdigest()
    print(hkey)

    if env['REQUEST_METHOD'] == 'GET':
        if not fc.exists(hkey):
            # key not in the FileCache
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [b'Key Not Found']

        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [fc.get(hkey)]

    if env['REQUEST_METHOD'] == 'PUT':
        flen = int(env.get('CONTENT_LENGTH', 0))
        if flen > 0:
            fc.put(hkey, env['wsgi.input'].read(flen))
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [b'']
        else:
            start_response('411 Length Required', [('Content-Type', 'text/plain')])
            return [b'']

    if env['REQUEST_METHOD'] == 'DELETE':
        fc.delete(hkey)
