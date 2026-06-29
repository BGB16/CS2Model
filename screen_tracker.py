"""
screen_tracker.py — Screen capture + OCR for live decimal odds tracking.

Captures defined screen regions, OCRs decimal odds from a betting feed,
tracks changes over time, and detects drastic moves.

Requirements:
  pip install mss pytesseract Pillow
  brew install tesseract
  Optional (much faster): pip install tesserocr
"""
import threading
import time
import re
import io
import os
import base64
import hashlib
import collections
import subprocess
from concurrent.futures import ThreadPoolExecutor

try:
    import mss
    import mss.tools
    HAS_MSS = True
except ImportError:
    HAS_MSS = False

try:
    from PIL import Image, ImageOps
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

_TESSDATA_PATH = None
try:
    import tesserocr
    HAS_TESSEROCR = True
    _candidate = os.environ.get('TESSDATA_PREFIX', '')
    if not _candidate or not os.path.isdir(_candidate):
        try:
            _prefix = subprocess.check_output(
                ['brew', '--prefix', 'tesseract'],
                stderr=subprocess.DEVNULL, timeout=2
            ).decode().strip()
            _candidate = os.path.join(_prefix, 'share', 'tessdata')
        except Exception:
            _candidate = '/opt/homebrew/opt/tesseract/share/tessdata'
    if os.path.isdir(_candidate):
        _TESSDATA_PATH = _candidate
    else:
        HAS_TESSEROCR = False
except ImportError:
    HAS_TESSEROCR = False

try:
    import pytesseract
    HAS_TESSERACT = True
except ImportError:
    HAS_TESSERACT = False


def _deps_available():
    return HAS_MSS and HAS_PIL and (HAS_TESSEROCR or HAS_TESSERACT)


def _missing_deps():
    missing = []
    if not HAS_MSS:
        missing.append('mss')
    if not HAS_PIL:
        missing.append('Pillow')
    if not HAS_TESSEROCR and not HAS_TESSERACT:
        missing.append('pytesseract + tesseract (or tesserocr for faster OCR)')
    return missing


class ScreenTracker:
    _OCR_POOL = ThreadPoolExecutor(max_workers=8)

    def __init__(self):
        self.scoreboard = None
        self.sub_regions = {}
        self.latest_state = {
            'odds': {},
            'raw_ocr': {},
            'confidence': {},
        }
        self._running = False
        self._thread = None
        self._interval = 1.0
        self._lock = threading.Lock()
        self._on_update = None
        self._history = collections.deque(maxlen=500)
        self._sct = None
        self._sct_lock = threading.Lock()
        self._crop_hashes = {}
        self._crop_cache = {}
        self._tess_api_local = threading.local()

    def _get_sct(self):
        if self._sct is None:
            self._sct = mss.mss()
        return self._sct

    def capture_full_screen(self, fast=False):
        with self._sct_lock:
            sct = self._get_sct()
            monitor = sct.monitors[1]
            img = sct.grab(monitor)
        pil_img = Image.frombytes('RGB', img.size, img.bgra, 'raw', 'BGRX')
        w, h = pil_img.size
        if fast:
            max_w = 800
            if w > max_w:
                scale = max_w / w
                pil_img = pil_img.resize((max_w, int(h * scale)), Image.NEAREST)
            buf = io.BytesIO()
            pil_img.save(buf, format='JPEG', quality=35)
        else:
            if w > 1920:
                scale = 1920 / w
                pil_img = pil_img.resize((1920, int(h * scale)), Image.LANCZOS)
            buf = io.BytesIO()
            pil_img.save(buf, format='PNG', optimize=True)
        return {
            'base64': base64.b64encode(buf.getvalue()).decode(),
            'format': 'jpeg' if fast else 'png',
            'width': monitor['width'],
            'height': monitor['height'],
            'display_width': pil_img.size[0],
            'display_height': pil_img.size[1],
        }

    def capture_region(self, x, y, w, h):
        with self._sct_lock:
            sct = self._get_sct()
            region = {'left': int(x), 'top': int(y), 'width': int(w), 'height': int(h)}
            img = sct.grab(region)
        return Image.frombytes('RGB', img.size, img.bgra, 'raw', 'BGRX')

    def set_scoreboard(self, region):
        with self._lock:
            self.scoreboard = region
            self.sub_regions = {}
        self._crop_hashes.clear()
        self._crop_cache.clear()

    def set_sub_region(self, name, region):
        with self._lock:
            self.sub_regions[name] = region

    def capture_scoreboard(self):
        if not self.scoreboard:
            return None
        r = self.scoreboard
        try:
            img = self.capture_region(r['x'], r['y'], r['w'], r['h'])
            buf = io.BytesIO()
            img.save(buf, format='JPEG', quality=80)
            return {
                'base64': base64.b64encode(buf.getvalue()).decode(),
                'width': r['w'],
                'height': r['h'],
                'pix_width': img.size[0],
                'pix_height': img.size[1],
            }
        except Exception as e:
            return {'error': str(e)}

    def _get_tess_api(self, whitelist=None):
        if not HAS_TESSEROCR:
            return None
        attr = '_api_odds' if whitelist else '_api_text'
        api = getattr(self._tess_api_local, attr, None)
        if api is None:
            api = tesserocr.PyTessBaseAPI(path=_TESSDATA_PATH, psm=tesserocr.PSM.SINGLE_LINE)
            if whitelist:
                api.SetVariable('tessedit_char_whitelist', whitelist)
            setattr(self._tess_api_local, attr, api)
        return api

    def _ocr_via_tesserocr(self, bw_img, whitelist=None):
        api = self._get_tess_api(whitelist=whitelist)
        if api is None:
            return ''
        api.SetImage(bw_img)
        return api.GetUTF8Text().strip()

    def _ocr_string(self, bw_img, cfg=None, whitelist=None):
        if HAS_TESSEROCR:
            return self._ocr_via_tesserocr(bw_img, whitelist=whitelist)
        return pytesseract.image_to_string(bw_img, config=cfg).strip()

    def ocr_decimal_odds(self, img):
        img = img.resize((img.width * 3, img.height * 3), Image.LANCZOS)
        gray = ImageOps.grayscale(img)
        cfg = '--psm 7 -c tessedit_char_whitelist=0123456789.'
        wl = '0123456789.'
        bw = gray.point(lambda x: 255 if x > 120 else 0)
        text = self._ocr_string(bw, cfg=cfg, whitelist=wl)
        m = re.search(r'(\d+\.\d{1,2})', text)
        if m:
            val = float(m.group(1))
            if 1.0 <= val <= 100.0:
                return val
        bw = ImageOps.invert(gray).point(lambda x: 255 if x > 120 else 0)
        text = self._ocr_string(bw, cfg=cfg, whitelist=wl)
        m = re.search(r'(\d+\.\d{1,2})', text)
        if m:
            val = float(m.group(1))
            if 1.0 <= val <= 100.0:
                return val
        return None

    def ocr_text(self, img):
        img = img.resize((img.width * 3, img.height * 3), Image.LANCZOS)
        gray = ImageOps.grayscale(img)
        cfg = '--psm 7'
        bw = gray.point(lambda x: 255 if x > 120 else 0)
        text = self._ocr_string(bw, cfg=cfg)
        if text:
            return text
        bw = ImageOps.invert(gray).point(lambda x: 255 if x > 120 else 0)
        text = self._ocr_string(bw, cfg=cfg)
        if text:
            return text
        return None

    @staticmethod
    def _crop_hash(crop):
        return hashlib.md5(crop.tobytes()).digest()

    def _ocr_sub_region(self, key, crop):
        h = self._crop_hash(crop)
        prev_hash = self._crop_hashes.get(key)
        if prev_hash == h and key in self._crop_cache:
            return key, self._crop_cache[key], 'cached'
        self._crop_hashes[key] = h

        if key.endswith('_label'):
            val = self.ocr_text(crop)
        else:
            val = self.ocr_decimal_odds(crop)
        self._crop_cache[key] = val
        return key, val, 'ok' if val is not None else 'failed'

    def capture_once(self):
        state = {}
        raw = {}
        conf = {}

        if not (self.scoreboard and self.sub_regions):
            return {'odds': {}, 'raw_ocr': raw, 'confidence': conf}

        sb = self.scoreboard
        try:
            sb_img = self.capture_region(sb['x'], sb['y'], sb['w'], sb['h'])
        except Exception as e:
            return {'odds': {}, 'raw_ocr': {'scoreboard': f'capture error: {e}'}, 'confidence': conf}

        pw, ph = sb_img.size
        sx = pw / sb['w'] if sb['w'] else 1
        sy = ph / sb['h'] if sb['h'] else 1

        crops = {}
        for key, sr in self.sub_regions.items():
            try:
                crops[key] = sb_img.crop((
                    int(sr['x'] * sx), int(sr['y'] * sy),
                    int((sr['x'] + sr['w']) * sx), int((sr['y'] + sr['h']) * sy),
                ))
            except Exception as e:
                raw[key] = f'crop error: {e}'
                conf[key] = 'error'

        futures = {
            key: self._OCR_POOL.submit(self._ocr_sub_region, key, crop)
            for key, crop in crops.items()
        }

        labels = {}
        for key, fut in futures.items():
            try:
                _, val, status = fut.result(timeout=5)
                raw[key] = val
                conf[key] = status
                if val is not None:
                    if key.endswith('_label'):
                        labels[key] = val
                    else:
                        state[key] = val
            except Exception as e:
                raw[key] = f'ocr error: {e}'
                conf[key] = 'error'

        with self._lock:
            self.latest_state['odds'] = dict(state)
            self.latest_state['labels'] = dict(labels)
            self.latest_state['raw_ocr'] = raw
            self.latest_state['confidence'] = conf
            result = dict(self.latest_state)

        if state:
            self._history.append({'ts': time.time(), 'odds': dict(state)})

        if self._on_update:
            try:
                self._on_update(result)
            except Exception:
                pass

        return result

    def reset(self):
        with self._lock:
            self.latest_state = {
                'odds': {},
                'raw_ocr': {},
                'confidence': {},
            }
        self._history.clear()
        self._crop_hashes.clear()
        self._crop_cache.clear()

    def remove_sub_regions(self, prefix):
        with self._lock:
            to_remove = [k for k in self.sub_regions if k.startswith(prefix)]
            for k in to_remove:
                del self.sub_regions[k]
                self._crop_hashes.pop(k, None)
                self._crop_cache.pop(k, None)

    def start(self, interval=1.0, on_update=None):
        if self._running:
            return
        self._interval = interval
        self._on_update = on_update
        self._running = True
        backend = 'tesserocr (in-process)' if HAS_TESSEROCR else 'pytesseract (subprocess)'
        print(f"[ScreenTracker] Starting with OCR backend: {backend}, interval: {interval}s")
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self):
        while self._running:
            t0 = time.monotonic()
            try:
                self.capture_once()
            except Exception as e:
                print(f"[ScreenTracker] Error: {e}")
            elapsed = time.monotonic() - t0
            remaining = self._interval - elapsed
            if remaining > 0:
                time.sleep(remaining)

    def get_state(self):
        with self._lock:
            return dict(self.latest_state)

    def get_history(self, last_n=100):
        return list(self._history)[-last_n:]

    @property
    def is_running(self):
        return self._running