#!/usr/bin/env python3
"""
سرور «انباریار پرو» — فقط پایتون استاندارد، بدون نصب هیچ چیز.

  python serve.py                 # پورت 8090، بدون کد
  python serve.py 8090 --key 1234 # با کد همگام‌سازی (برای چند گوشی)

دو کار می‌کند:
  ۱) خود برنامه را (بدون کش، تا نسخهٔ جدید همیشه بیاید) سرو می‌کند.
  ۲) اگر چند گوشی همگام باشند، داده‌ها را ادغام و در data/anbaryar.json نگه می‌دارد.
"""
import argparse, hmac, json, mimetypes, os, shutil, socket, sys, tempfile, threading, time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = '2.0'
ROOT = os.path.dirname(os.path.abspath(__file__))
DEAD_TTL_MS = 180 * 86400 * 1000
MAX_BODY = 60 * 1024 * 1024
COLS = ['products', 'moves', 'wh', 'cats', 'users', 'counts', 'log']
PUBLIC = {'/': 'index.html', '/index.html': 'index.html', '/manifest.webmanifest': 'manifest.webmanifest',
          '/sw.js': 'sw.js', '/Vazirmatn.woff2': 'Vazirmatn.woff2'}


# ------------------------------------------------ merge (همان منطق برنامه)
def _u(r):
    v = r.get('u', 0)
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def merge(a, b, now_ms=None):
    now_ms = now_ms or int(time.time() * 1000)
    dead = {}
    for src in (a.get('dead') or {}, b.get('dead') or {}):
        for k, v in src.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                dead[k] = max(dead.get(k, 0), v)
    out = {'v': 2}
    for col in COLS:
        best = {}
        for r in list(a.get(col) or []) + list(b.get(col) or []):
            if not isinstance(r, dict) or not r.get('id'):
                continue
            o = best.get(r['id'])
            if o is None or _u(r) > _u(o):
                best[r['id']] = r
        out[col] = []
        for r in best.values():
            d = dead.get(r['id'])
            if d and d >= _u(r):
                continue
            if d:
                del dead[r['id']]
            out[col].append(r)
    pids = {p['id'] for p in out['products']}
    out['moves'] = [m for m in out['moves'] if m.get('pid') in pids]
    out['log'] = sorted(out['log'], key=lambda x: x.get('ts', 0) if isinstance(x.get('ts', 0), (int, float)) else 0)[-1500:]
    for k in [k for k, v in dead.items() if now_ms - v > DEAD_TTL_MS]:
        del dead[k]
    out['dead'] = dead
    sa, sb = a.get('seq') or {}, b.get('seq') or {}
    out['seq'] = {k: max(sa.get(k, 0) or 0, sb.get(k, 0) or 0) for k in set(sa) | set(sb)}
    ta, tb = a.get('settings') or {}, b.get('settings') or {}
    t = tb if (tb.get('su') or 0) > (ta.get('su') or 0) else ta
    if not t.get('name') and (ta.get('name') or tb.get('name')):
        t = ta if ta.get('name') else tb
    out['settings'] = {'name': t.get('name'), 'su': t.get('su') or 0}
    return out


# ------------------------------------------------ storage
class Store:
    def __init__(self, path):
        self.path = path
        self.dir = os.path.dirname(path)
        os.makedirs(os.path.join(self.dir, 'backups'), exist_ok=True)
        self.lock = threading.Lock()
        self.rev = 0
        self.db = {'v': 2, 'dead': {}, 'seq': {}, 'settings': {}}
        for c in COLS:
            self.db[c] = []
        if os.path.exists(path):
            try:
                with open(path, encoding='utf-8') as f:
                    d = json.load(f)
                self.rev, self.db = int(d.get('rev', 0)), d['db']
            except Exception as e:
                shutil.copy(path, path + '.corrupt-%d' % time.time())
                print('⚠️  فایل داده خراب بود و کنار گذاشته شد:', e)

    def _write(self):
        fd, tmp = tempfile.mkstemp(dir=self.dir, suffix='.tmp')
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump({'rev': self.rev, 'db': self.db}, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        bk = os.path.join(self.dir, 'backups', 'anbaryar-%s.json' % time.strftime('%Y%m%d'))
        if not os.path.exists(bk):
            shutil.copy(self.path, bk)
            for n in sorted(os.listdir(os.path.dirname(bk)))[:-14]:
                try:
                    os.remove(os.path.join(os.path.dirname(bk), n))
                except OSError:
                    pass

    def sync(self, incoming):
        with self.lock:
            merged = merge(self.db, incoming)
            if json.dumps(merged, sort_keys=True) != json.dumps(merge(self.db, self.db), sort_keys=True):
                self.db = merged
                self.rev += 1
                self._write()
            return self.rev, self.db

    def snapshot(self):
        with self.lock:
            return self.rev, self.db


# ------------------------------------------------ http
class Handler(BaseHTTPRequestHandler):
    server_version = 'Anbaryar/1'
    store = None
    key = ''

    def log_message(self, fmt, *args):
        if '/api/rev' in self.path or '/api/ping' in self.path:
            return
        sys.stderr.write('%s  %s\n' % (time.strftime('%H:%M:%S'), fmt % args))

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Key')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Private-Network', 'true')

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        if not self.key:
            return True
        return hmac.compare_digest(self.headers.get('X-Key', '').encode(), self.key.encode())

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split('?', 1)[0]
        if path == '/api/ping':
            return self._json({'app': 'anbaryar', 'ver': VERSION, 'keyRequired': bool(self.key)})
        if path.startswith('/api/'):
            if not self._authed():
                return self._json({'error': 'unauthorized'}, 401)
            if path == '/api/rev':
                return self._json({'rev': self.store.snapshot()[0]})
            if path == '/api/db':
                rev, db = self.store.snapshot()
                return self._json({'rev': rev, 'db': db})
            return self._json({'error': 'not found'}, 404)
        rel = PUBLIC.get(path)
        if rel is None and path.startswith('/icons/') and '..' not in path and path.endswith('.png'):
            rel = path[1:]
        if rel is None:
            return self.send_error(HTTPStatus.NOT_FOUND)
        full = os.path.join(ROOT, rel)
        if not os.path.isfile(full):
            return self.send_error(HTTPStatus.NOT_FOUND)
        with open(full, 'rb') as f:
            data = f.read()
        ctype = mimetypes.guess_type(full)[0] or 'application/octet-stream'
        if rel.endswith('.webmanifest'):
            ctype = 'application/manifest+json'
        if rel.endswith('.js'):
            ctype = 'text/javascript'
        self.send_response(200)
        self.send_header('Content-Type', ctype + ('; charset=utf-8' if ctype.startswith('text') else ''))
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self._cors()
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        path = self.path.split('?', 1)[0]
        if path != '/api/sync':
            return self._json({'error': 'not found'}, 404)
        if not self._authed():
            return self._json({'error': 'unauthorized'}, 401)
        try:
            n = int(self.headers.get('Content-Length', '0'))
            if n <= 0 or n > MAX_BODY:
                return self._json({'error': 'bad size'}, 413)
            incoming = json.loads(self.rfile.read(n).decode('utf-8'))['db']
            if not isinstance(incoming, dict):
                raise ValueError
        except Exception:
            return self._json({'error': 'bad request'}, 400)
        rev, db = self.store.sync(incoming)
        self._json({'rev': rev, 'db': db})


def lan_ips():
    ips = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('10.255.255.255', 1))
        ips.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if not ip.startswith('127.'):
                ips.add(ip)
    except Exception:
        pass
    return sorted(ips)


def main():
    ap = argparse.ArgumentParser(description='Anbaryar Pro server')
    ap.add_argument('port', nargs='?', type=int, default=8090)
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--key', default=os.environ.get('ANBARYAR_KEY', ''), help='کد همگام‌سازی (اختیاری ولی پیشنهادی)')
    ap.add_argument('--data', default=os.path.join(ROOT, 'data', 'anbaryar.json'))
    a = ap.parse_args()
    Handler.store = Store(a.data)
    Handler.key = a.key
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    srv.daemon_threads = True
    print('\n📦 انباریار پرو روشن شد  (سرور %s)' % VERSION)
    print('   روی همین گوشی :  http://localhost:%d' % a.port)
    for ip in lan_ips():
        print('   گوشی‌های دیگر (همان Wi-Fi) برای همگام‌سازی:  http://%s:%d' % (ip, a.port))
    print('   کد همگام‌سازی  :', a.key or '— ندارد (پیشنهاد: --key 1234)')
    print('   توقف با Ctrl+C\n')
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\nخاموش شد.')


if __name__ == '__main__':
    main()
