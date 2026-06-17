"""
poly_cs2.py — Polymarket CS2 trading module.

Provides: PolyClient, CS2 market discovery, orderbook helpers, position tracking.
Ported from CBAModel/polymarket_edge.py, adapted for CS2.
"""
import json
import logging
import re
import time
import threading
import datetime
import requests

logging.getLogger("py_clob_client_v2.http_helpers.helpers").addFilter(
    lambda r: "heartbeats" not in getattr(r, "args", ("",))[1]
)

from kalshi_edge import match_kalshi_team

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
CHAIN_ID = 137


# ============================================================
# POLYMARKET CLIENT
# ============================================================

class PolyClient:
    """Thin wrapper around py-clob-client-v2 for Polymarket CLOB trading."""

    def __init__(self, private_key_path=None):
        self._client = None
        self._funder = None
        self._heartbeat_thread = None
        self._heartbeat_running = False
        if private_key_path:
            self._init_client(private_key_path)

    def _init_client(self, key_path):
        from py_clob_client_v2.client import ClobClient
        import py_clob_client_v2.http_helpers.helpers as _helpers
        import httpx
        _helpers._http_client = httpx.Client(http2=True, timeout=30)

        with open(key_path) as f:
            key = f.read().strip()
        self._funder = "0xb3736ecc788dd8859510378610ef146aa672d97c"
        self._client = ClobClient(
            CLOB_BASE,
            chain_id=CHAIN_ID,
            key=key,
            signature_type=1,
            funder=self._funder,
        )
        self._client.set_api_creds(self._client.derive_api_key())
        print(f"  Polymarket client initialized (V2)")

    def start_heartbeat(self):
        if self._heartbeat_running or not self._client:
            return
        self._heartbeat_running = True

        def _loop():
            hb_id = ""
            while self._heartbeat_running:
                try:
                    resp = self._client.post_heartbeat(hb_id)
                    if isinstance(resp, dict):
                        if resp.get("error_msg"):
                            hb_id = ""
                        else:
                            hb_id = resp.get("heartbeat_id", "")
                except Exception:
                    hb_id = ""
                time.sleep(10)

        self._heartbeat_thread = threading.Thread(target=_loop, daemon=True)
        self._heartbeat_thread.start()

    def stop_heartbeat(self):
        self._heartbeat_running = False

    def place_order(self, token_id, side, price, size, tick_size="0.01",
                    neg_risk=False, order_type="GTC", dry_run=False):
        """
        Place an order on Polymarket.
        price: float (e.g. 0.55)
        size: float (dollar amount)
        side: "BUY" or "SELL"
        """
        from py_clob_client_v2.clob_types import (
            OrderArgsV2, MarketOrderArgsV2, OrderType, PartialCreateOrderOptions,
        )

        if size < 5:
            print(f"    [Poly] Order size ${size:.2f} below $5 minimum — skipping")
            return None

        if dry_run:
            shares = size / price if price > 0 else 0
            label = f" {order_type}" if order_type != "GTC" else ""
            print(f"    [DRY RUN]{label} {side} ${size:.2f} @ {price:.2f} "
                  f"({shares:.1f} shares)")
            return {"id": "dry-run", "status": "dry-run", "size": size}

        ot = getattr(OrderType, order_type, OrderType.GTC)
        opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)

        try:
            if order_type in ("FOK", "FAK"):
                resp = self._client.create_and_post_market_order(
                    MarketOrderArgsV2(
                        token_id=token_id, amount=size,
                        side=side, price=price,
                    ), options=opts, order_type=ot)
            else:
                resp = self._client.create_and_post_order(
                    OrderArgsV2(
                        token_id=token_id, price=price,
                        size=size, side=side,
                    ), options=opts, order_type=ot)

            if isinstance(resp, dict):
                oid = resp.get("orderID", resp.get("id", "?"))
                status = resp.get("status", "?")
                print(f"    POLY ORDER (id={oid}, status={status})")
                return resp
            else:
                print(f"    POLY ORDER: {resp}")
                return {"id": str(resp), "status": "placed"}
        except Exception as e:
            print(f"    POLY ORDER FAILED: {e}")
            return None

    def cancel_all(self):
        if not self._client:
            return
        try:
            self._client.cancel_all()
            print("  [Poly] Cancelled all open orders")
        except Exception as e:
            print(f"  [Poly] Cancel all failed: {e}")

    def cancel_order(self, order_id):
        if not self._client or not order_id:
            return
        try:
            self._client.cancel(order_id)
        except Exception as e:
            print(f"  [Poly] Cancel order {order_id} failed: {e}")

    def cancel_token_orders(self, token_id):
        if not self._client:
            return 0
        try:
            from py_clob_client_v2.clob_types import OrderMarketCancelParams
            self._client.cancel_market_orders(
                OrderMarketCancelParams(asset_id=token_id))
            return 1
        except Exception as e:
            print(f"  [Poly] Cancel token orders failed: {e}")
            return 0

    def get_positions(self):
        try:
            if not self._funder:
                return []
            url = f"https://data-api.polymarket.com/positions?user={self._funder.lower()}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json() if isinstance(resp.json(), list) else []
            return []
        except Exception as e:
            print(f"  [Poly] Get positions failed: {e}")
            return []

    def get_position_ledger(self):
        """Build position ledger for CS2 markets.
        Returns {event_slug: {'cost': $, 'size': shares}}."""
        positions = self.get_positions()
        ledger = {}
        for p in positions:
            slug = p.get('slug', '')
            if not slug.startswith('cs2-'):
                continue
            cost = float(p.get('initialValue', 0))
            size = float(p.get('size', 0))
            event_slug = p.get('eventSlug', slug)
            if event_slug in ledger:
                ledger[event_slug]['cost'] += cost
                ledger[event_slug]['size'] += size
            else:
                ledger[event_slug] = {
                    'cost': cost,
                    'size': size,
                    'title': p.get('title', ''),
                    'slug': event_slug,
                }
        return ledger


# ============================================================
# MARKET DISCOVERY
# ============================================================

def _normalize_poly_market(mkt, event):
    """Convert a Gamma API market into our normalized format."""
    outcomes = json.loads(mkt.get('outcomes', '[]'))
    prices = json.loads(mkt.get('outcomePrices', '[]'))
    tokens = json.loads(mkt.get('clobTokenIds', '[]'))

    if len(outcomes) != 2 or len(tokens) != 2 or len(prices) != 2:
        return None

    game_start = None
    gs_str = mkt.get('gameStartTime', '')
    if gs_str:
        gs_clean = gs_str.strip().replace(' ', 'T')
        if re.match(r'.*[+-]\d{2}$', gs_clean):
            gs_clean += ':00'
        try:
            game_start = datetime.datetime.fromisoformat(gs_clean.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            pass

    if not game_start:
        cd = event.get('creationDate', '')
        if cd:
            try:
                game_start = datetime.datetime.fromisoformat(cd.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass

    return {
        'slug': event.get('slug', ''),
        'event_id': event.get('id', ''),
        'market_id': mkt.get('id', ''),
        'title': event.get('title', ''),
        'question': mkt.get('question', ''),
        'group_title': mkt.get('groupItemTitle', ''),
        'team_a': outcomes[0],
        'team_b': outcomes[1],
        'price_a': float(prices[0]) if prices[0] else 0,
        'price_b': float(prices[1]) if prices[1] else 0,
        'token_a': tokens[0],
        'token_b': tokens[1],
        'tick_size': str(mkt.get('orderPriceMinTickSize', '0.01')),
        'neg_risk': mkt.get('negRisk', False),
        'accepting_orders': mkt.get('acceptingOrders', True),
        'game_start_time': game_start,
        'end_date': mkt.get('endDate', ''),
        'liquidity': float(mkt.get('liquidityNum', 0) or 0),
        'volume': float(mkt.get('volumeNum', 0) or 0),
        'sports_market_type': mkt.get('sportsMarketType', ''),
        'home_team': None,
        'away_team': None,
    }


def fetch_poly_cs2_markets(pregame_only=True, today_only=True, our_teams=None):
    """Fetch open CS2 moneyline markets from Polymarket.

    pregame_only: skip live/started games (house mode)
    today_only:   only include games starting today (UTC)
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + datetime.timedelta(days=1)
    all_markets = []
    skipped = 0

    events = []
    try:
        for offset in range(0, 2000, 100):
            resp = requests.get(f"{GAMMA_BASE}/events",
                                params={'series_slug': 'counter-strike',
                                        'closed': 'false', 'limit': 100,
                                        'offset': offset},
                                timeout=15)
            if resp.status_code != 200:
                print(f"  [Poly] Gamma API error: {resp.status_code}")
                break
            batch = resp.json()
            events.extend(batch)
            if len(batch) < 100:
                break
    except Exception as e:
        print(f"  [Poly] Gamma API error: {e}")
        if not events:
            return []

    for event in events:
        slug = event.get('slug', '')
        if not slug.startswith('cs2-'):
            continue
        if event.get('closed'):
            continue

        for mkt in event.get('markets', []):
            if mkt.get('sportsMarketType') != 'moneyline':
                continue

            m = _normalize_poly_market(mkt, event)
            if m is None:
                continue

            prices = json.loads(mkt.get('outcomePrices', '[]'))
            if prices in [['0', '1'], ['1', '0']]:
                continue

            gs = m.get('game_start_time')

            if today_only:
                if not gs or not (today_start <= gs < tomorrow_start):
                    continue

            if our_teams:
                m['home_team'] = match_kalshi_team(m['team_a'], our_teams)
                m['away_team'] = match_kalshi_team(m['team_b'], our_teams)

            if pregame_only:
                if event.get('live'):
                    skipped += 1
                    continue
                if gs and now >= gs:
                    skipped += 1
                    continue

            all_markets.append(m)

    if skipped:
        print(f"  [Poly] Skipped {skipped} live/started markets")
    return all_markets


def _detect_game_number(market):
    """Detect which map/game number a Polymarket market represents.
    Returns 'game1', 'game2', 'game3', 'match', or None."""
    q = (market.get('question', '') + ' ' + market.get('group_title', '')).lower()
    title = market.get('title', '').lower()

    for text in [q, title]:
        m = re.search(r'game\s*(\d)', text)
        if m:
            return f"game{m.group(1)}"
        m = re.search(r'map\s*(\d)', text)
        if m:
            return f"game{m.group(1)}"

    smt = market.get('sports_market_type', '')
    if smt == 'moneyline':
        return 'match'

    return None


GAME_TO_MAP = {
    'game1': 'map1',
    'game2': 'map2',
    'game3': 'map3',
    'match': 'series',
}


def fetch_poly_cs2_live_markets(our_teams=None):
    """Fetch ALL open CS2 markets from Polymarket (match + map winners).
    No pregame/today filters — intended for live trading."""
    all_markets = []

    events = []
    try:
        for offset in range(0, 2000, 100):
            resp = requests.get(f"{GAMMA_BASE}/events",
                                params={'series_slug': 'counter-strike',
                                        'closed': 'false', 'limit': 100,
                                        'offset': offset},
                                timeout=15)
            if resp.status_code != 200:
                print(f"  [Poly CS2] Gamma API error: {resp.status_code}")
                break
            batch = resp.json()
            events.extend(batch)
            if len(batch) < 100:
                break
    except Exception as e:
        print(f"  [Poly CS2] Gamma API error: {e}")
        if not events:
            return []

    for event in events:
        slug = event.get('slug', '')
        if not slug.startswith('cs2-'):
            continue
        if event.get('closed'):
            continue

        for mkt in event.get('markets', []):
            m = _normalize_poly_market(mkt, event)
            if m is None:
                continue

            prices = json.loads(mkt.get('outcomePrices', '[]'))
            if prices in [['0', '1'], ['1', '0']]:
                continue

            game_type = _detect_game_number(m)
            m['game_type'] = game_type
            m['map_type'] = GAME_TO_MAP.get(game_type)

            if our_teams:
                m['home_team'] = match_kalshi_team(m['team_a'], our_teams)
                m['away_team'] = match_kalshi_team(m['team_b'], our_teams)

            all_markets.append(m)

    print(f"  [Poly CS2] Fetched {len(all_markets)} markets from {len(events)} events")
    return all_markets


def get_poly_cs2_tickers(poly_markets, our_teams=None):
    """Build map ticker lookup from Polymarket CS2 markets.
    Returns dict keyed by (home_team, away_team, map_type) with poly ticker info."""
    result = {}
    for m in poly_markets:
        map_type = m.get('map_type')
        if not map_type:
            continue

        home = m.get('home_team')
        away = m.get('away_team')
        if not home or not away:
            continue

        key = (home, away, map_type)
        if key not in result:
            result[key] = []

        result[key].append({
            'token_a': m['token_a'],
            'token_b': m['token_b'],
            'team_a': m['team_a'],
            'team_b': m['team_b'],
            'home_team': home,
            'away_team': away,
            'tick_size': m['tick_size'],
            'neg_risk': m['neg_risk'],
            'price_a': m['price_a'],
            'price_b': m['price_b'],
            'title': m['title'],
            'question': m.get('question', ''),
            'platform': 'poly',
        })
        print(f"  [Poly CS2] Map matched: {away} @ {home} {map_type} -> {m['title']}")

    return result


# ============================================================
# ORDERBOOK HELPERS
# ============================================================

def get_poly_orderbook(token_id):
    try:
        resp = requests.get(f"{CLOB_BASE}/book",
                            params={"token_id": token_id}, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {"bids": [], "asks": []}


def get_best_ask(orderbook):
    asks = orderbook.get("asks", [])
    if not asks:
        return 1.0
    return min(float(a.get("price", 0)) for a in asks if float(a.get("price", 0)) > 0)


def get_best_bid(orderbook):
    bids = orderbook.get("bids", [])
    if not bids:
        return 0.0
    return max(float(b.get("price", 0)) for b in bids)


# ============================================================
# POSITION TRACKING
# ============================================================

def get_poly_cs2_positions(client, our_teams):
    """Get CS2 positions from Polymarket, keyed by game_key (sorted team pair).
    Returns {game_key: total_shares}."""
    if not client:
        return {}
    ledger = client.get_position_ledger()
    if not ledger:
        return {}

    poly_markets = fetch_poly_cs2_markets(pregame_only=False, today_only=False, our_teams=our_teams)
    slug_to_game_key = {}
    for m in poly_markets:
        if m.get('home_team') and m.get('away_team'):
            gk = '|'.join(sorted([m['home_team'], m['away_team']]))
            slug_to_game_key[m['slug']] = gk

    positions = {}
    for slug, info in ledger.items():
        gk = slug_to_game_key.get(slug)
        if gk:
            positions[gk] = positions.get(gk, 0) + info['size']
    return positions


def build_poly_market_lookup(poly_markets):
    """Build lookup from game_key -> poly market dict."""
    lookup = {}
    for m in poly_markets:
        if m.get('home_team') and m.get('away_team'):
            gk = '|'.join(sorted([m['home_team'], m['away_team']]))
            if gk not in lookup:
                lookup[gk] = m
    return lookup