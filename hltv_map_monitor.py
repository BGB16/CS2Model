#!/usr/bin/env python3
"""
hltv_map_monitor.py — Monitor HLTV live matches and alert when a map finishes.

Uses a real Playwright browser to bypass Cloudflare (works on VPN).
Falls back to curl_cffi if Playwright is unavailable.

Usage:
    python3 hltv_map_monitor.py              # poll every 30s
    python3 hltv_map_monitor.py --interval 20
"""

import argparse
import subprocess
import time
from datetime import datetime

from bs4 import BeautifulSoup

MATCHES_URL = "https://www.hltv.org/matches"
BASE = "https://www.hltv.org"


CHROME_DEBUG_URL = "http://localhost:9222"


class HLTVBrowser:
    """Connect to Chrome running with --remote-debugging-port=9222."""

    def __init__(self):
        self._pw = None
        self._browser = None
        self._page = None

    def start(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.connect_over_cdp(CHROME_DEBUG_URL)
        ctx = self._browser.contexts[0] if self._browser.contexts else self._browser.new_context()
        self._page = ctx.new_page()
        print("  [HLTV] Connected to Chrome debug instance")

    def stop(self):
        if self._page:
            try:
                self._page.close()
            except Exception:
                pass
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
        if self._pw:
            self._pw.stop()
        self._pw = None
        self._browser = None
        self._page = None

    def get_page_html(self, url, timeout=20000):
        if not self._page:
            self.start()
        self._page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        return self._page.content()


_browser = None


def get_hltv_browser():
    global _browser
    if _browser is None:
        _browser = HLTVBrowser()
        _browser.start()
    return _browser


def close_hltv_browser():
    global _browser
    if _browser:
        _browser.stop()
        _browser = None


def load_browser_cookies(session):
    """Load HLTV cookies from Chrome (legacy fallback)."""
    try:
        import browser_cookie3
        cj = browser_cookie3.chrome(domain_name='.hltv.org')
        count = 0
        for c in cj:
            session.cookies.set(c.name, c.value, domain=c.domain)
            count += 1
        if count:
            print(f"  [HLTV] Loaded {count} cookies from Chrome")
        return count > 0
    except Exception:
        return False


def alert(message):
    print(f"\n*** ALERT [{datetime.now().strftime('%H:%M:%S')}] {message}")
    try:
        subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"])
    except Exception:
        print("\a")


def _parse_live_matches(html):
    """Parse live matches from HLTV /matches page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    matches = {}
    for wrapper in soup.select(".match-wrapper.live-match-container"):
        match_id = wrapper.get("data-match-id")
        if not match_id:
            continue
        link = wrapper.select_one("a[href*='/matches/']")
        if not link:
            continue
        team_names = [t.get_text(strip=True) for t in wrapper.select(".match-teamname")]
        bo_el = wrapper.select_one(".match-meta:not(.match-meta-live)")

        matches[match_id] = {
            "url": BASE + link["href"],
            "team1": team_names[0] if len(team_names) > 0 else "?",
            "team2": team_names[1] if len(team_names) > 1 else "?",
            "best_of": bo_el.get_text(strip=True) if bo_el else "",
        }
    return matches


def _parse_map_scores(html):
    """Parse map scores from HLTV match detail page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    maps = []
    for holder in soup.select("div.mapholder"):
        map_name_el = holder.select_one(".mapname")
        map_name = map_name_el.get_text(strip=True) if map_name_el else "?"
        scores = holder.select(".results-team-score")
        if len(scores) >= 2:
            s1 = scores[0].get_text(strip=True)
            s2 = scores[1].get_text(strip=True)
        else:
            s1, s2 = "-", "-"
        completed = holder.select_one("a[href*=stats]") is not None
        maps.append({"map": map_name, "t1": s1, "t2": s2, "completed": completed})
    return maps


def get_live_match_urls(session_or_browser=None, retries=2):
    """Fetch live matches. Uses Playwright browser if available, else curl_cffi session."""
    browser = None
    try:
        browser = get_hltv_browser()
    except Exception:
        pass

    if browser:
        try:
            html = browser.get_page_html(MATCHES_URL)
            return _parse_live_matches(html)
        except Exception as e:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] HLTV browser error: {e}")
            return {}

    # Fallback: curl_cffi session
    session = session_or_browser
    if session is None:
        return {}
    for attempt in range(retries + 1):
        resp = session.get(
            MATCHES_URL,
            impersonate="chrome131",
            timeout=20,
            headers={"Referer": "https://www.hltv.org"},
        )
        if resp.status_code == 403:
            if attempt == 0:
                load_browser_cookies(session)
                continue
            if attempt < retries:
                time.sleep(3 + attempt * 2)
                continue
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] HLTV 403 — blocked (VPN?), will retry next cycle")
            return {}
        resp.raise_for_status()
        return _parse_live_matches(resp.text)
    return {}


def get_map_scores(session_or_browser, match_url, retries=2):
    """Fetch map scores for a match. Uses Playwright browser if available."""
    browser = None
    try:
        browser = get_hltv_browser()
    except Exception:
        pass

    if browser:
        try:
            html = browser.get_page_html(match_url)
            return _parse_map_scores(html)
        except Exception as e:
            raise Exception(f"HLTV browser error: {e}")

    # Fallback: curl_cffi session
    session = session_or_browser
    for attempt in range(retries + 1):
        resp = session.get(
            match_url,
            impersonate="chrome131",
            timeout=20,
            headers={"Referer": MATCHES_URL},
        )
        if resp.status_code == 403:
            if attempt == 0:
                load_browser_cookies(session)
                continue
            if attempt < retries:
                time.sleep(3 + attempt * 2)
                continue
            raise Exception("HLTV 403 — blocked (VPN?)")
        resp.raise_for_status()
        return _parse_map_scores(resp.text)
    return []


def _is_map_won(s1, s2):
    """Check if round scores indicate a completed CS2 map.
    MR12: first to 13 win by 2. OT thresholds: 16, 19, 22, ... (every 3)."""
    if s1 == s2:
        return False
    high, low = max(s1, s2), min(s1, s2)
    if high < 13:
        return False
    if low < 12:
        return high >= 13
    # 12-12 or beyond: OT thresholds at 16, 19, 22, ...
    for threshold in range(16, high + 2, 3):
        if high == threshold and high - low >= 2:
            return True
    return False


def count_maps_won(map_scores):
    t1, t2 = 0, 0
    for m in map_scores:
        try:
            s1, s2 = int(m["t1"]), int(m["t2"])
        except (ValueError, TypeError):
            continue
        if m["completed"] or _is_map_won(s1, s2):
            if s1 > s2:
                t1 += 1
            elif s2 > s1:
                t2 += 1
    return t1, t2


def format_maps(map_scores):
    parts = []
    for m in map_scores:
        if m["t1"] == "-":
            parts.append(f"{m['map']}: -")
        else:
            parts.append(f"{m['map']}: {m['t1']}-{m['t2']}")
    return " | ".join(parts)


def main():
    parser = argparse.ArgumentParser(description="HLTV live map-count monitor")
    parser.add_argument("--interval", type=int, default=60, help="poll interval in seconds (default: 60)")
    args = parser.parse_args()

    print(f"HLTV Map Monitor — polling every {args.interval}s")
    print("Press Ctrl+C to stop.\n")

    print("  Starting browser...", end=" ", flush=True)
    get_hltv_browser()
    print("done.\n")

    prev_maps_won = {}
    match_info = {}

    while True:
        try:
            live = get_live_match_urls()

            if not live:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] No live matches found.", flush=True)
                time.sleep(args.interval)
                continue

            for mid in list(prev_maps_won):
                if mid not in live:
                    w = prev_maps_won.pop(mid)
                    info = match_info.pop(mid, {})
                    print(f"\n  Match ended: {info.get('team1','?')} vs {info.get('team2','?')}  "
                          f"final maps: {w[0]}-{w[1]}")

            for mid, info in live.items():
                time.sleep(1)
                try:
                    map_scores = get_map_scores(None, info["url"])
                except Exception as e:
                    print(f"\n  Error fetching {info['team1']} vs {info['team2']}: {e}")
                    continue

                w = count_maps_won(map_scores)
                match_info[mid] = info
                detail = format_maps(map_scores)

                if mid not in prev_maps_won:
                    prev_maps_won[mid] = w
                    print(f"  Tracking: {info['team1']} vs {info['team2']}  "
                          f"maps: {w[0]}-{w[1]}  ({info['best_of']})  {detail}")
                else:
                    old_w = prev_maps_won[mid]
                    if old_w != w:
                        msg = (f"Map finished! {info['team1']} vs {info['team2']}: "
                               f"maps {old_w[0]}-{old_w[1]} to {w[0]}-{w[1]}")
                        alert(msg)
                        print(f"  {detail}")
                        prev_maps_won[mid] = w

            ts = datetime.now().strftime("%H:%M:%S")
            summary = ", ".join(
                f"{match_info[mid]['team1']} {w[0]}-{w[1]} {match_info[mid]['team2']}"
                for mid, w in prev_maps_won.items() if mid in match_info
            )
            print(f"  [{ts}] {summary or 'no live matches'}", flush=True)

        except Exception as e:
            print(f"\n  Error: {e}")

        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        close_hltv_browser()