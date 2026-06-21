"""
live_trader.py — CS2 live in-game trading tool.

Browser UI for live CS2 match trading on Kalshi. Select a match, track map
scores and round scores, compute fair values, and post/cancel orders with
concurrent execution for minimal latency.

Usage:
    python3 live_trader.py --api-key-id KEY --private-key-path key.pem
    python3 live_trader.py --dry-run
    python3 live_trader.py --dry-run --port 5052
"""

import argparse
import json
import os
import sys
import time
import uuid
import datetime
import threading
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as http_requests
from flask import Flask, render_template_string, request, jsonify

from kalshi_edge import (
    load_private_key, make_auth_headers,
    KALSHI_BASE, SERIES_TICKER, MAP_SERIES_TICKER, TOTALMAPS_SERIES_TICKER,
    fetch_cs2_markets, extract_teams, detect_market_type,
    determine_yes_team, match_kalshi_team,
    load_model, get_win_prob, fetch_orderbook_best_ask,
)
from poly_cs2 import (
    PolyClient, get_poly_orderbook, get_best_ask as poly_best_ask,
    get_best_bid as poly_best_bid, fetch_poly_cs2_markets, GAMMA_BASE,
)
from screen_tracker import ScreenTracker, _deps_available as tracker_deps_available, _missing_deps as tracker_missing_deps

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

API_KEY_ID = None
PRIVATE_KEY = None
DRY_RUN = False
CONTRACTS = 50
SPREAD_CENTS = 6

PLACED_ORDER_IDS = []
PLACED_ORDER_LOCK = threading.Lock()

POLY_CLIENT = None
SCREEN_TRACKER = ScreenTracker()
FUTURES_TOKEN_IDS = []
FUTURES_TOKEN_LOCK = threading.Lock()
POLY_TOKEN_IDS = []
POLY_TOKEN_LOCK = threading.Lock()

KALSHI_WS = None
KALSHI_WS_FILLS = []
KALSHI_WS_FILLS_LOCK = threading.Lock()

app = Flask(__name__)


# ── Kalshi WebSocket Client ─────────────────────────────────────────

class KalshiWS:
    """Real-time Kalshi WebSocket for instant fill detection and cancel."""

    WS_URL = 'wss://api.elections.kalshi.com/trade-api/ws/v2'
    WS_PATH = '/trade-api/ws/v2'

    def __init__(self, api_key_id, private_key):
        self._api_key_id = api_key_id
        self._private_key = private_key
        self._ws = None
        self._thread = None
        self._running = False
        self._subscribed_tickers = set()
        self._msg_id = 0
        self._lock = threading.Lock()
        self._on_fill_callback = None
        self._reconnect_delay = 1
        self._connected = False

    @property
    def connected(self):
        return self._connected

    @property
    def subscribed_tickers(self):
        with self._lock:
            return set(self._subscribed_tickers)

    def start(self, on_fill=None):
        self._on_fill_callback = on_fill
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self._connected = False

    def _next_id(self):
        self._msg_id += 1
        return self._msg_id

    def subscribe(self, tickers):
        if not tickers:
            return
        new = [t for t in tickers if t not in self._subscribed_tickers]
        if not new:
            return
        with self._lock:
            self._subscribed_tickers.update(new)
        if self._ws and self._connected:
            self._send_subscribe(new)

    def unsubscribe(self, tickers):
        with self._lock:
            self._subscribed_tickers -= set(tickers)

    def _send_subscribe(self, tickers):
        try:
            self._ws.send(json.dumps({
                'id': self._next_id(),
                'cmd': 'subscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': tickers,
                }
            }))
            self._ws.send(json.dumps({
                'id': self._next_id(),
                'cmd': 'subscribe',
                'params': {
                    'channels': ['fill'],
                    'market_tickers': tickers,
                }
            }))
        except Exception as e:
            print(f"[KalshiWS] Subscribe error: {e}")

    def _make_ws_headers(self):
        return make_auth_headers(self._private_key, self._api_key_id, 'GET', self.WS_PATH)

    def _run_loop(self):
        import websocket as ws_lib

        while self._running:
            try:
                hdrs = self._make_ws_headers()
                header_list = [f"{k}: {v}" for k, v in hdrs.items()]
                self._ws = ws_lib.create_connection(
                    self.WS_URL,
                    header=header_list,
                    timeout=30,
                )
                self._connected = True
                self._reconnect_delay = 1
                print(f"[KalshiWS] Connected")

                with self._lock:
                    tickers = list(self._subscribed_tickers)
                if tickers:
                    self._send_subscribe(tickers)

                while self._running:
                    try:
                        raw = self._ws.recv()
                        if raw:
                            self._handle_message(json.loads(raw))
                    except ws_lib.WebSocketTimeoutException:
                        continue
                    except ws_lib.WebSocketConnectionClosedException:
                        print(f"[KalshiWS] Connection closed")
                        break
                    except Exception as e:
                        print(f"[KalshiWS] Recv error: {e}")
                        break
            except Exception as e:
                print(f"[KalshiWS] Connection error: {e}")

            self._connected = False
            if self._running:
                print(f"[KalshiWS] Reconnecting in {self._reconnect_delay}s...")
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 30)

    def _handle_message(self, data):
        msg_type = data.get('type', '')
        if msg_type == 'subscribed':
            channel = data.get('msg', {}).get('channel')
            sid = data.get('msg', {}).get('sid')
            print(f"[KalshiWS] Subscribed: {channel} (sid={sid})")
        elif msg_type == 'fill':
            self._handle_fill(data.get('msg', {}))
        elif msg_type == 'orderbook_delta':
            self._handle_ob_delta(data.get('msg', {}))
        elif msg_type == 'error':
            err = data.get('msg', {})
            print(f"[KalshiWS] Error: {err.get('msg', '')} (code={err.get('code', '')})")

    def _handle_fill(self, msg):
        ticker = msg.get('market_ticker', '')
        count = float(msg.get('count_fp', '0'))
        price = msg.get('yes_price_dollars', '')
        side = msg.get('side', '')
        is_taker = msg.get('is_taker', False)
        order_id = msg.get('order_id', '')

        print(f"[KalshiWS] FILL: {ticker} {side} {count:.0f}@{price} "
              f"{'TAKER' if is_taker else 'MAKER'} oid={order_id[:8]}")

        with KALSHI_WS_FILLS_LOCK:
            KALSHI_WS_FILLS.append({
                'ticker': ticker, 'side': side, 'count': count,
                'price': price, 'is_taker': is_taker,
                'order_id': order_id, 'ts': time.time(),
            })
            if len(KALSHI_WS_FILLS) > 100:
                KALSHI_WS_FILLS[:] = KALSHI_WS_FILLS[-100:]

        if self._on_fill_callback:
            try:
                self._on_fill_callback(msg)
            except Exception as e:
                print(f"[KalshiWS] Fill callback error: {e}")

    def _handle_ob_delta(self, msg):
        ticker = msg.get('market_ticker', '')
        price_dollars = msg.get('price_dollars', '')
        delta_fp = msg.get('delta_fp', '')
        side = msg.get('side', '')
        client_oid = msg.get('client_order_id')

        if not client_oid:
            return

        price_cents = int(round(float(price_dollars) * 100))
        delta = float(delta_fp)

        if delta < 0:
            with PLACED_ORDER_LOCK:
                our_oids = [o for o in PLACED_ORDER_IDS if o['ticker'] == ticker]
            for o in our_oids:
                our_yes_price = o['yes_price']
                if abs(our_yes_price - price_cents) <= 1:
                    print(f"[KalshiWS] OB hit at {price_cents}c on {ticker} — "
                          f"canceling remaining orders")
                    self._fire_cancel(ticker)
                    break

    def _fire_cancel(self, ticker):
        event_key = '-'.join(ticker.split('-')[:-1])
        with PLACED_ORDER_LOCK:
            event_oids = [o['oid'] for o in PLACED_ORDER_IDS
                          if '-'.join(o['ticker'].split('-')[:-1]) == event_key]
        if not event_oids:
            return
        def _do_cancel():
            cancelled = 0
            with ThreadPoolExecutor(max_workers=8) as pool:
                futs = {pool.submit(_cancel_single_order, oid): oid for oid in event_oids}
                for f in as_completed(futs):
                    if f.result():
                        cancelled += 1
            with PLACED_ORDER_LOCK:
                cancelled_set = set(event_oids)
                PLACED_ORDER_IDS[:] = [o for o in PLACED_ORDER_IDS
                                       if o['oid'] not in cancelled_set]
            print(f"[KalshiWS] Cancelled {cancelled}/{len(event_oids)} orders "
                  f"on {event_key}")
        threading.Thread(target=_do_cancel, daemon=True).start()


def _ws_fill_handler(fill_msg):
    """On maker fill: cancel remaining resting orders on that event to prevent adverse fills."""
    is_taker = fill_msg.get('is_taker', False)
    ticker = fill_msg.get('market_ticker', '')
    if is_taker or not ticker:
        return
    event_key = '-'.join(ticker.split('-')[:-1])
    with PLACED_ORDER_LOCK:
        event_oids = [o['oid'] for o in PLACED_ORDER_IDS
                      if '-'.join(o['ticker'].split('-')[:-1]) == event_key
                      and o['ticker'] != ticker]
    if not event_oids:
        return
    print(f"[KalshiWS] Maker fill on {ticker} — canceling {len(event_oids)} "
          f"other orders on {event_key}")
    def _do():
        cancelled = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_cancel_single_order, oid): oid for oid in event_oids}
            for f in as_completed(futs):
                if f.result():
                    cancelled += 1
        with PLACED_ORDER_LOCK:
            cancelled_set = set(event_oids)
            PLACED_ORDER_IDS[:] = [o for o in PLACED_ORDER_IDS
                                   if o['oid'] not in cancelled_set]
        print(f"[KalshiWS] Defensive cancel done: {cancelled}/{len(event_oids)}")
    threading.Thread(target=_do, daemon=True).start()


# ── CS2 Weapon & Economy Data ────────────────────────────────────────
# HLTV scorebot weapon names -> game data
# cost: buy price, kill_reward: per-kill bonus, power: 0-1 combat rating

WEAPONS = {
    # --- Pistols ---
    'glock':           {'cost': 200,  'kill_reward': 300, 'power': 0.15, 'side': 'T',    'cat': 'pistol'},
    'hkp2000':         {'cost': 200,  'kill_reward': 300, 'power': 0.15, 'side': 'CT',   'cat': 'pistol'},
    'usp_silencer':    {'cost': 200,  'kill_reward': 300, 'power': 0.20, 'side': 'CT',   'cat': 'pistol'},
    'elite':           {'cost': 300,  'kill_reward': 300, 'power': 0.15, 'side': 'both', 'cat': 'pistol'},
    'p250':            {'cost': 300,  'kill_reward': 300, 'power': 0.22, 'side': 'both', 'cat': 'pistol'},
    'tec9':            {'cost': 500,  'kill_reward': 300, 'power': 0.25, 'side': 'T',    'cat': 'pistol'},
    'fn57':            {'cost': 500,  'kill_reward': 300, 'power': 0.25, 'side': 'CT',   'cat': 'pistol'},
    'cz75a':           {'cost': 500,  'kill_reward': 100, 'power': 0.25, 'side': 'both', 'cat': 'pistol'},
    'deagle':          {'cost': 700,  'kill_reward': 300, 'power': 0.40, 'side': 'both', 'cat': 'pistol'},
    'revolver':        {'cost': 600,  'kill_reward': 300, 'power': 0.30, 'side': 'both', 'cat': 'pistol'},
    # --- SMGs ---
    'mac10':           {'cost': 1050, 'kill_reward': 600, 'power': 0.35, 'side': 'T',    'cat': 'smg'},
    'mp9':             {'cost': 1250, 'kill_reward': 600, 'power': 0.38, 'side': 'CT',   'cat': 'smg'},
    'mp7':             {'cost': 1500, 'kill_reward': 600, 'power': 0.35, 'side': 'both', 'cat': 'smg'},
    'mp5sd':           {'cost': 1500, 'kill_reward': 600, 'power': 0.33, 'side': 'both', 'cat': 'smg'},
    'ump45':           {'cost': 1200, 'kill_reward': 600, 'power': 0.36, 'side': 'both', 'cat': 'smg'},
    'p90':             {'cost': 2350, 'kill_reward': 300, 'power': 0.42, 'side': 'both', 'cat': 'smg'},
    'bizon':           {'cost': 1400, 'kill_reward': 600, 'power': 0.28, 'side': 'both', 'cat': 'smg'},
    # --- Rifles ---
    'famas':           {'cost': 1950, 'kill_reward': 300, 'power': 0.55, 'side': 'CT',   'cat': 'rifle'},
    'galilar':         {'cost': 1800, 'kill_reward': 300, 'power': 0.52, 'side': 'T',    'cat': 'rifle'},
    'ak47':            {'cost': 2700, 'kill_reward': 300, 'power': 0.85, 'side': 'T',    'cat': 'rifle'},
    'm4a1':            {'cost': 2900, 'kill_reward': 300, 'power': 0.82, 'side': 'CT',   'cat': 'rifle'},
    'm4a1_silencer':   {'cost': 2900, 'kill_reward': 300, 'power': 0.83, 'side': 'CT',   'cat': 'rifle'},
    'sg556':           {'cost': 3000, 'kill_reward': 300, 'power': 0.78, 'side': 'T',    'cat': 'rifle'},
    'aug':             {'cost': 3300, 'kill_reward': 300, 'power': 0.78, 'side': 'CT',   'cat': 'rifle'},
    # --- Snipers ---
    'ssg08':           {'cost': 1700, 'kill_reward': 300, 'power': 0.55, 'side': 'both', 'cat': 'sniper'},
    'awp':             {'cost': 4750, 'kill_reward': 100, 'power': 0.95, 'side': 'both', 'cat': 'sniper'},
    'g3sg1':           {'cost': 5000, 'kill_reward': 300, 'power': 0.70, 'side': 'T',    'cat': 'sniper'},
    'scar20':          {'cost': 5000, 'kill_reward': 300, 'power': 0.70, 'side': 'CT',   'cat': 'sniper'},
    # --- Shotguns ---
    'nova':            {'cost': 1050, 'kill_reward': 900, 'power': 0.20, 'side': 'both', 'cat': 'shotgun'},
    'xm1014':          {'cost': 2000, 'kill_reward': 900, 'power': 0.25, 'side': 'both', 'cat': 'shotgun'},
    'sawedoff':        {'cost': 1100, 'kill_reward': 900, 'power': 0.18, 'side': 'T',    'cat': 'shotgun'},
    'mag7':            {'cost': 1300, 'kill_reward': 900, 'power': 0.28, 'side': 'CT',   'cat': 'shotgun'},
    # --- Machine guns ---
    'negev':           {'cost': 1700, 'kill_reward': 300, 'power': 0.35, 'side': 'both', 'cat': 'mg'},
    'm249':            {'cost': 5200, 'kill_reward': 300, 'power': 0.40, 'side': 'both', 'cat': 'mg'},
    # --- Utility / defaults ---
    'knife':           {'cost': 0,    'kill_reward': 1500,'power': 0.02, 'side': 'both', 'cat': 'melee'},
    'knife_t':         {'cost': 0,    'kill_reward': 1500,'power': 0.02, 'side': 'T',    'cat': 'melee'},
    'knife_ct':        {'cost': 0,    'kill_reward': 1500,'power': 0.02, 'side': 'CT',   'cat': 'melee'},
    'taser':           {'cost': 200,  'kill_reward': 300, 'power': 0.01, 'side': 'both', 'cat': 'other'},
    'c4':              {'cost': 0,    'kill_reward': 300, 'power': 0.00, 'side': 'T',    'cat': 'other'},
}

# HLTV sometimes uses alternate names
WEAPON_ALIASES = {
    'usps': 'usp_silencer', 'usp': 'usp_silencer', 'usp_silencer_off': 'usp_silencer',
    'm4a1_silencer_off': 'm4a1_silencer', 'm4a4': 'm4a1',
    'p2000': 'hkp2000', 'fiveseven': 'fn57', 'dualelite': 'elite', 'duals': 'elite',
    'galil': 'galilar', 'scout': 'ssg08', 'sg553': 'sg556', 'krieg': 'sg556',
    'mp5_sd': 'mp5sd', 'bayonet': 'knife', 'karambit': 'knife',
}

# Equipment costs
EQUIPMENT = {
    'kevlar': 650,
    'kevlar_helmet': 1000,
    'defusekit': 400,
}

# Round loss bonus ladder (consecutive losses)
LOSS_BONUS = [1400, 1900, 2400, 2900, 3400]
# Round win reward
WIN_REWARD = 3250
# Bomb plant bonus (T side, even on loss)
PLANT_BONUS = 800

MAX_MONEY = 16000
START_MONEY = 800  # pistol round
OT_START_MONEY = 10000  # most tournament OT formats

MAP_CT_RATES = {
    'mirage':  0.55,
    'inferno': 0.53,
    'nuke':    0.57,
    'anubis':  0.52,
    'ancient': 0.54,
    'dust2':   0.51,
    'vertigo': 0.54,
    'train':   0.53,
}


def get_weapon_data(weapon_name):
    """Look up weapon data by HLTV scorebot name."""
    name = weapon_name.lower().strip()
    name = WEAPON_ALIASES.get(name, name)
    return WEAPONS.get(name)


def team_loadout_value(players):
    """Compute total equipment value and average power for a team's loadout."""
    total_value = 0
    total_power = 0.0
    alive = 0
    for p in players:
        if not p.get('alive', True):
            continue
        alive += 1
        wdata = get_weapon_data(p.get('weapon_name', ''))
        if wdata:
            total_value += wdata['cost']
            total_power += wdata['power']
        else:
            total_power += 0.15  # unknown weapon, assume pistol-tier
        armor = p.get('armor_img', '')
        if 'kevlar_helmet' in armor:
            total_value += 1000
        elif 'kevlar' in armor:
            total_value += 650
        if p.get('has_defuse'):
            total_value += 400
    avg_power = total_power / max(alive, 1)
    total_money = sum(p.get('money', 0) for p in players)
    avg_money = total_money / max(len(players), 1)
    return {
        'alive': alive,
        'total_value': total_value,
        'avg_power': round(avg_power, 3),
        'total_power': round(total_power, 3),
        'total_money': total_money,
        'avg_money': int(avg_money),
    }


# Buy classification thresholds (from avg weapon power)
FULL_BUY_POWER = 0.65
FORCE_BUY_POWER = 0.35

# Neutral round win rate by buy matchup (no team skill)
BUY_MATCHUP_RATES = {
    ('full',  'full'):  0.50,
    ('full',  'force'): 0.78,
    ('full',  'eco'):   0.92,
    ('force', 'full'):  0.22,
    ('force', 'force'): 0.50,
    ('force', 'eco'):   0.68,
    ('eco',   'full'):  0.08,
    ('eco',   'force'): 0.32,
    ('eco',   'eco'):   0.50,
}


def classify_buy(avg_power):
    if avg_power >= FULL_BUY_POWER:
        return 'full'
    if avg_power >= FORCE_BUY_POWER:
        return 'force'
    return 'eco'


# Mid-round win rate by alive player counts [ct_alive][t_alive]
# Based on historical CS2 data — man advantage is enormous
ALIVE_ADVANTAGE = {
    (5,5): 0.50, (5,4): 0.80, (5,3): 0.93, (5,2): 0.98, (5,1): 0.995,
    (4,5): 0.20, (4,4): 0.50, (4,3): 0.80, (4,2): 0.93, (4,1): 0.98,
    (3,5): 0.07, (3,4): 0.20, (3,3): 0.50, (3,2): 0.80, (3,1): 0.93,
    (2,5): 0.02, (2,4): 0.07, (2,3): 0.20, (2,2): 0.50, (2,1): 0.80,
    (1,5): 0.005,(1,4): 0.02, (1,3): 0.07, (1,2): 0.20, (1,1): 0.50,
}


def classify_buy_from_money(avg_money, side):
    """Predict next-round buy level from money."""
    threshold = 3700 if side == 't' else 3900
    if avg_money >= threshold:
        return 'full'
    if avg_money >= 2400:
        return 'force'
    return 'eco'


def detect_round_phase(ct_players, t_players, timer_seconds=-1):
    """Detect current round phase.

    Returns:
        'round_over' — one team has 0 alive, use projected money for next round
        'buy_phase'  — everyone alive but only default weapons, money is unreliable
                        (players actively spending), skip economy adjustment
        'live'       — round in progress, use weapons/alive/HP
    """
    ct_alive = sum(1 for p in ct_players if p.get('alive', True))
    t_alive = sum(1 for p in t_players if p.get('alive', True))
    if ct_alive == 0 or t_alive == 0:
        return 'round_over'
    if ct_alive == 5 and t_alive == 5:
        all_players = [p for p in ct_players + t_players if p.get('alive', True)]
        all_low_weapons = all(
            _weapon_is_default(p.get('weapon_name', '')) for p in all_players
        )
        if all_low_weapons:
            return 'buy_phase'
    return 'live'


def _weapon_is_default(weapon_name):
    """Check if weapon is a default/spawn weapon (pistol, knife, C4, or empty)."""
    if not weapon_name:
        return True
    w = get_weapon_data(weapon_name)
    if not w:
        return True
    return w['cat'] in ('pistol', 'melee', 'other')


def economy_adjusted_map_prob(ct_players, t_players, base_map_prob,
                               home_is_ct, home_rounds, away_rounds,
                               bomb_planted=False, timer_seconds=-1,
                               round_history=None, ct_round_rate=None):
    """Adjust live map probability based on economy/weapons/bomb/timer.

    Computes:
      P(map) = P(win this round) * P(map | home_rounds+1, away_rounds)
             + P(lose this round) * P(map | home_rounds, away_rounds+1)

    Handles:
    - Between-rounds: uses money to predict next-round buys (not stale weapons)
    - Near halftime: reduces impact since economy resets at side switch
    - OT: both teams start with $10k, economy less relevant
    """
    if not ct_players and not t_players:
        return base_map_prob, None

    # Between-maps detection: no real players or absurd money (lobby/warmup state)
    all_players = ct_players + t_players
    max_money = max((p.get('money', 0) for p in all_players), default=0)
    alive_count = sum(1 for p in all_players if p.get('alive', True))
    if max_money >= 100000 or alive_count == 0 or len(all_players) < 2:
        return base_map_prob, None

    total_rounds = home_rounds + away_rounds
    rounds_to_half = 12 - max(home_rounds, away_rounds) if total_rounds < 24 else 99
    in_ot = total_rounds >= 24

    ct_info = team_loadout_value(ct_players)
    t_info = team_loadout_value(t_players)

    phase = detect_round_phase(ct_players, t_players, timer_seconds)

    ct_alive = ct_info['alive']
    t_alive = t_info['alive']
    skill_p = base_map_prob if home_is_ct else (1 - base_map_prob)
    ct_display_money = ct_info['avg_money']
    t_display_money = t_info['avg_money']

    if phase == 'buy_phase':
        # Players actively buying — money dropping as they spend.
        # Weapons haven't loaded yet. Skip all adjustment, use base prob.
        return base_map_prob, {
            'home_buy': '?', 'away_buy': '?',
            'home_avg_money': ct_display_money if home_is_ct else t_display_money,
            'away_avg_money': t_display_money if home_is_ct else ct_display_money,
            'home_avg_power': 0, 'away_avg_power': 0,
            'home_alive': 5, 'away_alive': 5,
            'home_is_ct': home_is_ct,
            'phase': 'buy_phase',
            'bomb_planted': False, 'bomb_status': 'none',
            'timer_seconds': timer_seconds,
            'econ_weight': 0, 'home_round_p': 0.5,
            'econ_map_prob': round(base_map_prob, 4),
        }

    if phase == 'round_over':
        # Round just ended — project NEXT round money based on who won/lost.
        # HLTV shows money before round rewards are applied, so we need to
        # add win bonus / loss bonus + kill rewards to predict the buy.
        ct_alive_n = sum(1 for p in ct_players if p.get('alive', True))
        t_alive_n = sum(1 for p in t_players if p.get('alive', True))

        # Figure out who won the round (team with 0 alive lost)
        if ct_alive_n == 0 and t_alive_n > 0:
            ct_won = False
        elif t_alive_n == 0 and ct_alive_n > 0:
            ct_won = True
        else:
            # Buy phase (both alive) — money shown is post-reward, use as-is
            ct_won = None

        ct_avg = ct_info['avg_money']
        t_avg = t_info['avg_money']

        if ct_won is not None:
            # Count consecutive losses from round history for accurate loss bonus
            def _loss_streak_from_history(loser_side, rh):
                """Count consecutive losses for loser_side from most recent rounds."""
                streak = 0
                for r in reversed(rh):
                    if r.get('winner') == loser_side:
                        break
                    if r.get('winner') and r['winner'] != loser_side:
                        streak += 1
                    else:
                        break
                return streak

            def _had_bomb_plant(rh, round_num):
                """Check if bomb was planted in a specific round from history."""
                for r in rh:
                    if r.get('round') == round_num:
                        return r.get('win_type') in ('bomb', 'defuse')
                return False

            ct_loss_streak = 0
            t_loss_streak = 0
            if round_history and len(round_history) > 0:
                ct_loss_streak = _loss_streak_from_history('ct', round_history)
                t_loss_streak = _loss_streak_from_history('t', round_history)

            def _get_loss_bonus(side, loser_avg):
                """Get loss bonus from round history streak or estimate from money."""
                streak = ct_loss_streak if side == 'ct' else t_loss_streak
                if streak > 0:
                    return LOSS_BONUS[min(streak - 1, len(LOSS_BONUS) - 1)]
                if loser_avg >= 3000:
                    return LOSS_BONUS[0]
                elif loser_avg >= 2000:
                    return LOSS_BONUS[1]
                elif loser_avg >= 1000:
                    return LOSS_BONUS[2]
                else:
                    return LOSS_BONUS[3]

            # Check bomb plant from round history if not detected live
            rh = round_history or []
            cur_round = home_rounds + away_rounds
            bomb_in_round = bomb_planted or _had_bomb_plant(rh, cur_round)

            ct_projected = []
            for p in ct_players:
                m = p.get('money', 0)
                if ct_won:
                    m += WIN_REWARD
                else:
                    m += _get_loss_bonus('ct', ct_avg)
                ct_projected.append(min(MAX_MONEY, m))

            t_projected = []
            for p in t_players:
                m = p.get('money', 0)
                if not ct_won:
                    m += WIN_REWARD
                    if bomb_in_round:
                        m += PLANT_BONUS
                else:
                    m += _get_loss_bonus('t', t_avg)
                    if bomb_in_round:
                        m += PLANT_BONUS
                t_projected.append(min(MAX_MONEY, m))

            ct_avg = sum(ct_projected) / max(len(ct_projected), 1)
            t_avg = sum(t_projected) / max(len(t_projected), 1)
            ct_display_money = int(ct_avg)
            t_display_money = int(t_avg)

            # Build projected player lists so forward-looking code uses next-round money
            ct_players = [dict(p, money=ct_projected[i]) for i, p in enumerate(ct_players)]
            t_players = [dict(p, money=t_projected[i]) for i, p in enumerate(t_players)]

        ct_buy = classify_buy_from_money(ct_avg, 'ct')
        t_buy = classify_buy_from_money(t_avg, 't')

        matchup_rate = BUY_MATCHUP_RATES.get((ct_buy, t_buy), 0.50)
        matchup_edge = abs(matchup_rate - 0.50)
        econ_weight = 0.55 + matchup_edge * 1.2
        econ_weight = min(econ_weight, 0.92)

        if 0 <= rounds_to_half <= 1 and not in_ot:
            econ_weight *= 0.3
        elif 0 <= rounds_to_half <= 2 and not in_ot:
            econ_weight *= 0.5
        if in_ot:
            econ_weight *= 0.4

        ct_round_p = (1 - econ_weight) * skill_p + econ_weight * matchup_rate
    else:
        # Mid-round — factor in alive count, HP, AND individual weapons
        ct_buy = classify_buy(ct_info['avg_power'])
        t_buy = classify_buy(t_info['avg_power'])
        # Compute continuous weapon advantage from total team firepower
        ct_total_power = ct_info['total_power']
        t_total_power = t_info['total_power']
        total_power = ct_total_power + t_total_power
        if total_power > 0:
            weapon_ratio = ct_total_power / total_power
            # Aggressive conversion: ratio^1.5 curve to amplify gaps
            # 0.276 ratio (eco vs full) -> ~0.145 -> weapon_rate ~0.15
            if weapon_ratio >= 0.5:
                scaled = 0.5 + 0.5 * ((weapon_ratio - 0.5) / 0.5) ** 0.7
            else:
                scaled = 0.5 - 0.5 * ((0.5 - weapon_ratio) / 0.5) ** 0.7
            weapon_rate = max(0.03, min(0.97, scaled))
        else:
            weapon_rate = 0.50

        # Alive advantage — biggest mid-round factor
        ct_a = max(1, min(5, ct_alive))
        t_a = max(1, min(5, t_alive))
        alive_rate = ALIVE_ADVANTAGE.get((ct_a, t_a), 0.50)

        # HP adjustment
        ct_hp = sum(p.get('hp', 0) for p in ct_players if p.get('alive', True))
        t_hp = sum(p.get('hp', 0) for p in t_players if p.get('alive', True))
        total_hp = ct_hp + t_hp
        hp_shift = 0.0
        if total_hp > 0 and ct_alive > 0 and t_alive > 0:
            hp_ratio = ct_hp / total_hp
            hp_shift = (hp_ratio - 0.5) * 0.40
            alive_rate = max(0.01, min(0.99, alive_rate + hp_shift))

        if ct_alive == 5 and t_alive == 5:
            # Full 5v5 — weapons are the primary differentiator
            weapon_gap = abs(weapon_rate - 0.50)
            weapon_weight = min(0.92, 0.65 + weapon_gap * 1.2)
            ct_round_p = (1 - weapon_weight) * skill_p + weapon_weight * weapon_rate
            ct_round_p = max(0.01, min(0.99, ct_round_p + hp_shift))
        else:
            # Players dead — alive count dominates more with bigger gaps
            death_count = (5 - ct_alive) + (5 - t_alive)
            player_diff = abs(ct_alive - t_alive)
            # More deaths + bigger gap = alive rate takes over almost completely
            alive_pct = min(0.98, 0.70 + death_count * 0.06 + player_diff * 0.08)
            # Weapons matter less when outnumbered — scale down with player diff
            weapon_pct = max(0.02, 1.0 - alive_pct)
            situation_rate = alive_pct * alive_rate + weapon_pct * weapon_rate
            # Skill matters almost nothing in a 1v3+
            skill_weight = max(0.01, 0.15 - player_diff * 0.04)
            ct_round_p = skill_weight * skill_p + (1 - skill_weight) * situation_rate

        econ_weight = 0.0  # tracked for display only in mid-round

        if 0 <= rounds_to_half <= 1 and not in_ot:
            ct_round_p = 0.7 * ct_round_p + 0.3 * skill_p
        if in_ot:
            ct_round_p = 0.6 * ct_round_p + 0.4 * skill_p

    # ── Bomb & Timer adjustment ──
    # Bomb planted: T wins if timer runs out (40s fuse). CT must defuse (5s or 10s with kit).
    # No bomb + low timer: CT wins if time expires (T must plant or get kills).
    bomb_status = 'none'
    if phase == 'live' and timer_seconds >= 0:
        if bomb_planted:
            bomb_status = 'planted'
            # Bomb down — heavily favors T
            # CT needs: alive players, defuse kit, time to defuse, clear site
            ct_has_kit = any(p.get('has_defuse') for p in ct_players if p.get('alive'))
            defuse_time = 5 if ct_has_kit else 10

            if timer_seconds <= defuse_time:
                # Not enough time to defuse even in best case — T wins
                ct_round_p = 0.01
            elif timer_seconds <= defuse_time + 5:
                # Barely enough time — T heavily favored
                ct_round_p = max(0.02, ct_round_p * 0.15)
            elif timer_seconds <= 20:
                # Tight but possible — T favored, depends on alive count
                t_bomb_edge = 0.75 + (20 - timer_seconds) * 0.012
                ct_round_p = ct_round_p * (1 - t_bomb_edge)
            else:
                # Bomb down but plenty of time — moderate T edge
                t_bomb_edge = 0.35
                ct_round_p = ct_round_p * (1 - t_bomb_edge)
        else:
            # No bomb planted
            if timer_seconds <= 10:
                bomb_status = 'time_low'
                # Under 10s, no bomb — T must plant or get kills NOW
                # Heavily favors CT
                ct_round_p = max(ct_round_p, 0.85 + (10 - timer_seconds) * 0.014)
                ct_round_p = min(ct_round_p, 0.99)
            elif timer_seconds <= 25:
                bomb_status = 'time_mid'
                # Getting tight for T — moderate CT edge from time pressure
                time_pressure = (25 - timer_seconds) / 25.0  # 0 at 25s, 1 at 0s
                ct_boost = time_pressure * 0.20
                ct_round_p = min(0.99, ct_round_p + ct_boost * (1 - ct_round_p))

    ct_round_p = max(0.01, min(0.99, ct_round_p))
    home_round_p = ct_round_p if home_is_ct else (1 - ct_round_p)

    # ── Forward-looking economy projection ──
    # Project what happens if home wins vs loses this round: what buys next
    # round, and what does the map probability look like 2 rounds out?
    # This runs for ALL phases so the user always sees the forward picture.

    home_info_l = ct_info if home_is_ct else t_info
    away_info_l = t_info if home_is_ct else ct_info
    home_total_money = home_info_l['total_money']
    away_total_money = away_info_l['total_money']
    home_avg_money = home_info_l['avg_money']
    away_avg_money = away_info_l['avg_money']

    def _project_team_buy(players, won, loss_streak, bomb_plant_bonus=False):
        """Project each player's next-round money and classify team buy."""
        projected = []
        for p in players:
            m = p.get('money', 0)
            if won:
                m += WIN_REWARD
            else:
                m += LOSS_BONUS[min(loss_streak, len(LOSS_BONUS) - 1)]
            if bomb_plant_bonus:
                m += PLANT_BONUS
            projected.append(min(MAX_MONEY, m))
        if not projected:
            return 'eco', 0
        avg = sum(projected) / len(projected)
        can_full = sum(1 for m in projected if m >= 3900)
        if can_full >= 3:
            buy = 'full'
        elif avg >= 2400:
            buy = 'force'
        else:
            buy = 'eco'
        return buy, int(avg)

    home_loss_streak = 0 if home_avg_money > 3500 else (1 if home_avg_money > 2000 else 2)
    away_loss_streak = 0 if away_avg_money > 3500 else (1 if away_avg_money > 2000 else 2)

    home_ct_players = ct_players if home_is_ct else t_players
    away_ct_players = t_players if home_is_ct else ct_players

    rh = round_history or []
    cur_round = home_rounds + away_rounds
    bomb_in_round = bomb_planted or any(
        r.get('round') == cur_round and r.get('win_type') in ('bomb', 'defuse') for r in rh)
    t_plant_bonus = bomb_in_round

    # If home wins: home gets win reward, away gets loss bonus
    hw_buy, hw_money = _project_team_buy(home_ct_players, True, 0)
    al_buy, al_money = _project_team_buy(
        away_ct_players, False, away_loss_streak + 1,
        bomb_plant_bonus=(t_plant_bonus and not home_is_ct))

    # If home loses: away gets win reward, home gets loss bonus
    hl_buy, hl_money = _project_team_buy(
        home_ct_players, False, home_loss_streak + 1,
        bomb_plant_bonus=(t_plant_bonus and home_is_ct))
    aw_buy, aw_money = _project_team_buy(away_ct_players, True, 0)

    def _side_adjusted_matchup(ct_buy_level, t_buy_level):
        """Buy matchup rate adjusted for CT/T side advantage."""
        base = BUY_MATCHUP_RATES.get((ct_buy_level, t_buy_level), 0.50)
        if ct_round_rate is not None and base == 0.50:
            return ct_round_rate
        if ct_round_rate is not None:
            side_shift = (ct_round_rate - 0.50) * 0.6
            return max(0.01, min(0.99, base + side_shift))
        return base

    # Next-round matchup rates (CT perspective)
    if home_is_ct:
        r2_ct_if_won = _side_adjusted_matchup(hw_buy, al_buy)
        r2_ct_if_lost = _side_adjusted_matchup(hl_buy, aw_buy)
    else:
        r2_ct_if_won = _side_adjusted_matchup(al_buy, hw_buy)
        r2_ct_if_lost = _side_adjusted_matchup(aw_buy, hl_buy)
    r2_home_if_won = r2_ct_if_won if home_is_ct else (1 - r2_ct_if_won)
    r2_home_if_lost = r2_ct_if_lost if home_is_ct else (1 - r2_ct_if_lost)

    # Blend next-round rate with skill
    skill_home_p = base_map_prob if home_is_ct else (1 - base_map_prob)
    r2_home_p_if_won = 0.5 * r2_home_if_won + 0.5 * skill_home_p
    r2_home_p_if_lost = 0.5 * r2_home_if_lost + 0.5 * skill_home_p

    # 1-round lookahead (score-only)
    p_map_if_win = live_round_win_prob(home_rounds + 1, away_rounds, base_map_prob,
                                       home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
    p_map_if_lose = live_round_win_prob(home_rounds, away_rounds + 1, base_map_prob,
                                         home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)

    # 2-round lookahead (score + projected economy)
    p_map_win_win = live_round_win_prob(home_rounds + 2, away_rounds, base_map_prob,
                                         home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
    p_map_win_lose = live_round_win_prob(home_rounds + 1, away_rounds + 1, base_map_prob,
                                          home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
    p_map_after_win = r2_home_p_if_won * p_map_win_win + (1 - r2_home_p_if_won) * p_map_win_lose

    p_map_lose_win = live_round_win_prob(home_rounds + 1, away_rounds + 1, base_map_prob,
                                          home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
    p_map_lose_lose = live_round_win_prob(home_rounds, away_rounds + 2, base_map_prob,
                                           home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
    p_map_after_lose = r2_home_p_if_lost * p_map_lose_win + (1 - r2_home_p_if_lost) * p_map_lose_lose

    # Blend 1-round and 2-round: always use 2-round, weighted more when economy matters
    if total_rounds < 24:
        expected_map_p = home_round_p * p_map_after_win + (1 - home_round_p) * p_map_after_lose
    else:
        expected_map_p = home_round_p * p_map_if_win + (1 - home_round_p) * p_map_if_lose

    home_info = ct_info if home_is_ct else t_info
    away_info = t_info if home_is_ct else ct_info

    home_alive = ct_alive if home_is_ct else t_alive
    away_alive = t_alive if home_is_ct else ct_alive

    econ_detail = {
        'home_buy': ct_buy if home_is_ct else t_buy,
        'away_buy': t_buy if home_is_ct else ct_buy,
        'home_avg_money': ct_display_money if home_is_ct else t_display_money,
        'away_avg_money': t_display_money if home_is_ct else ct_display_money,
        'home_total_money': home_total_money,
        'away_total_money': away_total_money,
        'home_avg_power': home_info['avg_power'],
        'away_avg_power': away_info['avg_power'],
        'home_alive': home_alive,
        'away_alive': away_alive,
        'home_is_ct': home_is_ct,
        'phase': phase,
        'bomb_planted': bomb_planted,
        'bomb_status': bomb_status,
        'timer_seconds': timer_seconds,
        'econ_weight': round(econ_weight, 2),
        'home_round_p': round(home_round_p, 4),
        'econ_map_prob': round(expected_map_p, 4),
        # Forward-looking: projected buys and map probs for each outcome
        'home_buy_if_win': hw_buy,
        'away_buy_if_win': al_buy,
        'home_money_if_win': hw_money,
        'away_money_if_win': al_money,
        'home_buy_if_lose': hl_buy,
        'away_buy_if_lose': aw_buy,
        'home_money_if_lose': hl_money,
        'away_money_if_lose': aw_money,
        'map_prob_if_win': round(p_map_after_win, 4),
        'map_prob_if_lose': round(p_map_after_lose, 4),
    }

    return expected_map_p, econ_detail


# ── Probability ──────────────────────────────────────────────────────

def power_devig(home_decimal, away_decimal):
    """Devig two-way decimal odds using the power method.
    Returns (home_fair_prob, away_fair_prob) as floats 0-1."""
    imp_h = 1.0 / home_decimal
    imp_a = 1.0 / away_decimal
    lo, hi = 1.0, 10.0
    for _ in range(100):
        mid = (lo + hi) / 2.0
        total = imp_h ** mid + imp_a ** mid
        if total > 1.0:
            lo = mid
        else:
            hi = mid
    k = (lo + hi) / 2.0
    fair_h = imp_h ** k
    fair_a = imp_a ** k
    s = fair_h + fair_a
    return fair_h / s, fair_a / s



def series_to_map_prob(series_prob):
    """Derive per-map win probability from a BO3 series win probability.
    Inverts the BO3 formula: finds p such that P(win BO3 | map_prob=p) = series_prob."""
    clamped = max(0.01, min(0.99, series_prob))
    try:
        from scipy.optimize import brentq
    except ImportError:
        return clamped

    def _bo3_from_map(p):
        return p * p * (3 - 2 * p)

    try:
        return brentq(lambda p: _bo3_from_map(p) - clamped, 0.001, 0.999)
    except Exception:
        return clamped


def live_bo3_win_prob(team1_maps, team2_maps, maps_played, per_map_probs):
    """BO3 series win probability via DP from current map score.
    per_map_probs = [p_map1, p_map2, p_map3] for team1."""
    if team1_maps >= 2:
        return 1.0
    if team2_maps >= 2:
        return 0.0

    remaining = per_map_probs[maps_played:]
    if not remaining:
        return 0.5

    def _recurse(idx, h, a, prob):
        if h >= 2:
            return prob
        if a >= 2:
            return 0.0
        if idx >= len(remaining):
            return 0.0
        p = remaining[idx]
        return (_recurse(idx + 1, h + 1, a, prob * p) +
                _recurse(idx + 1, h, a + 1, prob * (1 - p)))

    return _recurse(0, team1_maps, team2_maps, 1.0)


def series_to_map_prob_bo5(series_prob):
    """Derive per-map win probability from a BO5 series win probability.
    Inverts the BO5 formula: finds p such that P(win BO5 | map_prob=p) = series_prob."""
    clamped = max(0.01, min(0.99, series_prob))
    try:
        from scipy.optimize import brentq
    except ImportError:
        return clamped

    def _bo5_from_map(p):
        # P(win BO5) = p^3 * [1 + 3(1-p) + 6(1-p)^2]
        q = 1 - p
        return p**3 * (1 + 3*q + 6*q*q)

    try:
        return brentq(lambda p: _bo5_from_map(p) - clamped, 0.001, 0.999)
    except Exception:
        return clamped


def live_bo5_win_prob(team1_maps, team2_maps, maps_played, per_map_probs):
    """BO5 series win probability via DP from current map score.
    per_map_probs = [p_map1, ..., p_map5] for team1."""
    if team1_maps >= 3:
        return 1.0
    if team2_maps >= 3:
        return 0.0

    remaining = per_map_probs[maps_played:]
    if not remaining:
        return 0.5

    def _recurse(idx, h, a, prob):
        if h >= 3:
            return prob
        if a >= 3:
            return 0.0
        if idx >= len(remaining):
            return 0.0
        p = remaining[idx]
        return (_recurse(idx + 1, h + 1, a, prob * p) +
                _recurse(idx + 1, h, a + 1, prob * (1 - p)))

    return _recurse(0, team1_maps, team2_maps, 1.0)


def _ot_win_prob(p):
    """Probability of winning a single MR3 overtime set (first to 4, win by 2).
    If 3-3, a new OT set starts — modeled as the same p winning that next set,
    giving P(win OT) = P(win set outright) / (1 - P(3-3))."""
    from math import comb
    memo = {}

    def _rec(h, a):
        if h >= 4 and h - a >= 2:
            return 1.0
        if a >= 4 and a - h >= 2:
            return 0.0
        if h == 3 and a == 3:
            return None  # tie — new OT set
        if h + a >= 6:
            diff = h - a
            if diff >= 2:
                return 1.0
            if diff <= -2:
                return 0.0
            return None  # 3-3 tie
        if (h, a) in memo:
            return memo[(h, a)]
        w = _rec(h + 1, a)
        l = _rec(h, a + 1)
        if w is None and l is None:
            memo[(h, a)] = None
            return None
        # If one branch is a tie (None), that branch restarts OT with the same P
        # We solve: P = p*w + (1-p)*l where None branches become P itself
        # So: P = p*(w if w is not None else P) + (1-p)*(l if l is not None else P)
        w_val = w
        l_val = l
        p_coeff = 0.0  # coefficient of P on RHS
        const = 0.0
        if w_val is None:
            p_coeff += p
        else:
            const += p * w_val
        if l_val is None:
            p_coeff += (1 - p)
        else:
            const += (1 - p) * l_val
        if p_coeff >= 1.0:
            memo[(h, a)] = 0.5
            return 0.5
        result = const / (1.0 - p_coeff)
        memo[(h, a)] = result
        return result

    return _rec(0, 0) or 0.5


def live_round_win_prob(team1_rounds, team2_rounds, base_map_prob,
                        home_is_ct=None, ct_round_rate=None):
    """Live map win probability given round score.
    Regulation: MR12, first to 13 wins. If 12-12 → overtime.
    OT: MR3 sets (6 rounds), win by 2. If 3-3 in OT set, new set starts.
    Derives per-round win rate from base_map_prob via brentq.

    Side-aware: when home_is_ct and ct_round_rate are provided, uses
    different p_round for each half. A team going 3-0 on T side of a
    CT-sided map gets a bigger boost than 3-0 on CT side because they're
    outperforming expectation on the weaker side."""
    has_ct_adj = home_is_ct is not None and ct_round_rate is not None
    if team1_rounds == 0 and team2_rounds == 0 and not has_ct_adj:
        return base_map_prob

    try:
        from scipy.optimize import brentq
    except ImportError:
        total = team1_rounds + team2_rounds
        lead = team1_rounds - team2_rounds
        progress = min(total / 24.0, 1.0)
        return max(0.01, min(0.99, base_map_prob + lead * progress * 0.04))

    def _map_win_prob_full(p_round):
        """P(win map from 0-0) including OT."""
        memo = {}

        def _reg(h, a):
            if h >= 13 and h > a:
                return 1.0
            if a >= 13 and a > h:
                return 0.0
            if h == 12 and a == 12:
                return _ot_win_prob(p_round)
            if (h, a) in memo:
                return memo[(h, a)]
            memo[(h, a)] = p_round * _reg(h + 1, a) + (1 - p_round) * _reg(h, a + 1)
            return memo[(h, a)]

        return _reg(0, 0)

    clamped = max(0.01, min(0.99, base_map_prob))
    try:
        p_round = brentq(lambda p: _map_win_prob_full(p) - clamped, 0.01, 0.99)
    except Exception:
        p_round = clamped

    # Side-aware round probabilities: shift the neutral p_round by the map's
    # CT/T asymmetry. Going 3-0 on T side of a 57% CT map is outperforming
    # expectation far more than 3-0 on CT side.
    if has_ct_adj:
        side_shift = ct_round_rate - 0.50
        p_home_ct = max(0.01, min(0.99, p_round + side_shift))
        p_home_t = max(0.01, min(0.99, p_round - side_shift))
    else:
        p_home_ct = p_round
        p_home_t = p_round

    p_ot = p_round

    def _p_for_round(total_r):
        """Home's round win probability based on which half/side they're on."""
        if total_r < 12:
            return p_home_ct if home_is_ct else p_home_t
        elif total_r < 24:
            return p_home_t if home_is_ct else p_home_ct
        return p_ot

    def _from_state(h, a):
        if h < 12 or a < 12:
            if h >= 13:
                return 1.0
            if a >= 13:
                return 0.0
            memo = {}

            def _reg(rh, ra):
                if rh >= 13:
                    return 1.0
                if ra >= 13:
                    return 0.0
                if rh == 12 and ra == 12:
                    return _ot_win_prob(p_ot)
                if (rh, ra) in memo:
                    return memo[(rh, ra)]
                p = _p_for_round(rh + ra)
                memo[(rh, ra)] = (p * _reg(rh + 1, ra) +
                                  (1 - p) * _reg(rh, ra + 1))
                return memo[(rh, ra)]

            return _reg(h, a)

        # In OT: both >= 12
        if h == 12 and a == 12:
            return _ot_win_prob(p_ot)

        ot_h = h - 12
        ot_a = a - 12
        rh, ra = ot_h, ot_a
        while rh >= 3 and ra >= 3:
            rh -= 3
            ra -= 3

        ot_memo = {}

        def _ot_rec(oh, oa):
            if oh + oa >= 6:
                diff = oh - oa
                if diff >= 2:
                    return 1.0
                if diff <= -2:
                    return 0.0
                return _ot_win_prob(p_ot)
            remaining = 6 - oh - oa
            if oh - oa > remaining:
                return 1.0
            if oa - oh > remaining:
                return 0.0
            if (oh, oa) in ot_memo:
                return ot_memo[(oh, oa)]
            ot_memo[(oh, oa)] = (p_ot * _ot_rec(oh + 1, oa) +
                                 (1 - p_ot) * _ot_rec(oh, oa + 1))
            return ot_memo[(oh, oa)]

        return _ot_rec(rh, ra)

    return max(0.01, min(0.99, _from_state(team1_rounds, team2_rounds)))


def compute_alt_lines_bo3(home_maps, away_maps, maps_played, per_map_probs):
    """Enumerate remaining BO3 outcomes for scoreline probabilities."""
    remaining = per_map_probs[maps_played:]
    outcomes = {}

    def _enumerate(idx, h, a, prob):
        if h >= 2 or a >= 2 or idx >= len(remaining):
            key = (h, a)
            outcomes[key] = outcomes.get(key, 0) + prob
            return
        p = remaining[idx]
        _enumerate(idx + 1, h + 1, a, prob * p)
        _enumerate(idx + 1, h, a + 1, prob * (1 - p))

    _enumerate(0, home_maps, away_maps, 1.0)

    scorelines = []
    for label in ['2-0', '2-1', '1-2', '0-2']:
        h, a = map(int, label.split('-'))
        prob = outcomes.get((h, a), 0)
        scorelines.append({
            'label': label,
            'prob': round(prob, 4),
            'fair': int(round(prob * 100)),
        })

    over = outcomes.get((2, 1), 0) + outcomes.get((1, 2), 0)
    under = outcomes.get((2, 0), 0) + outcomes.get((0, 2), 0)
    over_under = {
        'over_prob': round(over, 4),
        'under_prob': round(under, 4),
        'over_fair': int(round(over * 100)),
        'under_fair': int(round(under * 100)),
    }

    return {'scorelines': scorelines, 'over_under': over_under}


# ── Kalshi market matching ───────────────────────────────────────────

def get_kalshi_cs2_tickers():
    """Fetch open CS2 match-winner and totalmaps markets and match to model predictions."""
    try:
        markets = fetch_cs2_markets(pregame_only=False,
                                    series_filter=[SERIES_TICKER, TOTALMAPS_SERIES_TICKER])
    except Exception as e:
        print(f"[Live Trader] Error fetching markets: {e}")
        return {}

    model_path = os.path.join(DATA_DIR, 'model', 'model.pkl')
    if os.path.exists(model_path):
        model, encoders, scale = load_model()
        team_names = list(encoders['team'].categories_[0])
    else:
        print("[Live Trader] No model — showing markets at 50%")
        model, encoders, scale, team_names = None, None, 1.0, []

    result = {}
    for m in markets:
        away_k, home_k = extract_teams(m)
        if not away_k or not home_k:
            continue

        if team_names:
            away_matched = match_kalshi_team(away_k, team_names)
            home_matched = match_kalshi_team(home_k, team_names)
            if not away_matched or not home_matched:
                away_matched, home_matched = away_k, home_k
        else:
            away_matched, home_matched = away_k, home_k

        ticker = m.get('ticker', '')
        mtype = detect_market_type(m)

        key = (home_matched, away_matched)
        if key not in result:
            prob = 0.5
            if model and encoders:
                try:
                    prob, _, _ = get_win_prob(model, encoders, home_matched, away_matched, scale)
                except Exception:
                    pass
            result[key] = {
                'home': home_matched, 'away': away_matched,
                'home_prob': round(float(prob), 4),
                'tickers': [],
                'ou_tickers': [],
            }

        if mtype == 'totalmaps':
            result[key]['ou_tickers'].append({
                'ticker': ticker,
                'yes_team': 'Over 2.5',
                'no_team': 'Under 2.5',
            })
            print(f"[Live Trader] O/U Matched: {away_matched} vs {home_matched} -> {ticker}")
        else:
            yes_is_home = determine_yes_team(m, home_matched, away_matched)
            if yes_is_home is None:
                yes_is_home = True
            result[key]['tickers'].append({
                'ticker': ticker,
                'home_is_yes': yes_is_home,
                'yes_team': home_matched if yes_is_home else away_matched,
                'no_team': away_matched if yes_is_home else home_matched,
            })
            print(f"[Live Trader] Matched: {away_matched} vs {home_matched} -> {ticker}")

    return result


# ── Concurrent order management ──────────────────────────────────────

def place_tracked_order(ticker, side, count, price_cents):
    yes_price = price_cents if side == 'yes' else 100 - price_cents

    if DRY_RUN:
        print(f"      [DRY RUN] BID {count} {side.upper()} @ {price_cents}c")
        with PLACED_ORDER_LOCK:
            PLACED_ORDER_IDS.append({
                'oid': f"dry-{uuid.uuid4()}", 'ticker': ticker,
                'side': side, 'yes_price': yes_price,
            })
        return True

    path = '/trade-api/v2/portfolio/events/orders'
    url = KALSHI_BASE + '/portfolio/events/orders'
    v2_side = 'bid' if side == 'yes' else 'ask'
    v2_price = f"{yes_price / 100:.4f}"
    order = {
        'ticker': ticker, 'side': v2_side,
        'count': str(count), 'price': v2_price,
        'client_order_id': f"cs2-live-{uuid.uuid4()}",
        'time_in_force': 'good_till_canceled',
        'self_trade_prevention_type': 'maker',
        'post_only': True,
    }
    for attempt in range(4):
        headers = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'POST', path)
        try:
            resp = http_requests.post(url, headers=headers, json=order, timeout=10)
        except Exception as e:
            print(f"      FAILED: {e}")
            return False
        if resp.status_code == 201:
            oid = resp.json().get('order_id', '?')
            print(f"      ORDER {oid} {side.upper()} @ {price_cents}c")
            with PLACED_ORDER_LOCK:
                PLACED_ORDER_IDS.append({
                    'oid': oid, 'ticker': ticker,
                    'side': side, 'yes_price': yes_price,
                })
            return True
        if resp.status_code == 429:
            time.sleep(0.3 * (attempt + 1))
            continue
        print(f"      FAILED: {resp.status_code} {resp.text[:200]}")
        return False
    print(f"      FAILED: rate limited after 4 retries")
    return False


def place_ioc_order(ticker, side, count, price_cents):
    if DRY_RUN:
        print(f"      [DRY RUN] IOC {count} {side.upper()} @ {price_cents}c")
        return 0, count

    path = '/trade-api/v2/portfolio/events/orders'
    url = KALSHI_BASE + '/portfolio/events/orders'
    yes_price = price_cents if side == 'yes' else 100 - price_cents
    v2_side = 'bid' if side == 'yes' else 'ask'
    v2_price = f"{yes_price / 100:.4f}"
    order = {
        'ticker': ticker, 'side': v2_side,
        'count': str(count), 'price': v2_price,
        'client_order_id': f"cs2-ioc-{uuid.uuid4()}",
        'time_in_force': 'immediate_or_cancel',
        'self_trade_prevention_type': 'maker',
    }
    headers = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'POST', path)
    try:
        resp = http_requests.post(url, headers=headers, json=order, timeout=10)
    except Exception as e:
        print(f"      IOC FAILED: {e}")
        return 0, count
    if resp.status_code == 201:
        data = resp.json()
        filled = int(float(data.get('fill_count', '0')))
        if filled > 0:
            print(f"      IOC FILLED {filled}/{count} @ {price_cents}c")
        else:
            print(f"      IOC NOT FILLED @ {price_cents}c")
        return filled, count
    print(f"      IOC FAILED: {resp.status_code} {resp.text[:200]}")
    return 0, count


def _cancel_single_order(oid):
    for attempt in range(3):
        path = f'/trade-api/v2/portfolio/events/orders/{oid}'
        url = KALSHI_BASE + f'/portfolio/events/orders/{oid}'
        hdrs = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'DELETE', path)
        try:
            r = http_requests.delete(url, headers=hdrs, timeout=10)
            if r.status_code in (200, 204):
                return True
            if r.status_code == 429:
                time.sleep(0.5 * (attempt + 1))
                continue
            return False
        except Exception:
            return False
    return False


def cancel_all_live_orders(tickers=None):
    """Cancel all resting orders concurrently using tracked IDs first, API fallback."""
    if DRY_RUN:
        with PLACED_ORDER_LOCK:
            PLACED_ORDER_IDS.clear()
        return 0, "DRY RUN — orders cleared"

    ticker_set = set(tickers) if tickers else None

    with PLACED_ORDER_LOCK:
        if ticker_set:
            tracked = [o['oid'] for o in PLACED_ORDER_IDS if o['ticker'] in ticker_set]
            PLACED_ORDER_IDS[:] = [o for o in PLACED_ORDER_IDS if o['ticker'] not in ticker_set]
        else:
            tracked = [o['oid'] for o in PLACED_ORDER_IDS]
            PLACED_ORDER_IDS.clear()

    if tracked:
        cancelled = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            for f in as_completed({pool.submit(_cancel_single_order, oid): oid for oid in tracked}):
                if f.result():
                    cancelled += 1
        msg = f"Cancelled {cancelled}/{len(tracked)} tracked orders"
        print(f"[Cancel] {msg}")
        return 1, msg

    if not tickers:
        return 1, "No tickers provided"

    def _fetch_resting(ticker):
        path = '/trade-api/v2/portfolio/orders'
        url = KALSHI_BASE + '/portfolio/orders'
        params = {'status': 'resting', 'ticker': ticker}
        headers = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'GET', path)
        try:
            resp = http_requests.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code == 200:
                return [o.get('order_id', '') for o in resp.json().get('orders', [])]
        except Exception:
            pass
        return []

    all_oids = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed({pool.submit(_fetch_resting, t): t for t in tickers}):
            all_oids.extend(f.result())

    if not all_oids:
        return 1, "No resting orders found"

    print(f"[Cancel] {len(all_oids)} orders across {len(tickers)} tickers (API fallback)...")
    cancelled = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        for f in as_completed({pool.submit(_cancel_single_order, oid): oid for oid in all_oids}):
            if f.result():
                cancelled += 1

    msg = f"Cancelled {cancelled}/{len(all_oids)} orders"
    print(f"[Cancel] {msg}")
    return 1, msg


LAYER_COUNT = 4


def _ev_bid(fair_cents, min_ev_pct):
    """Compute bid price for a target EV%. Formula: (fair - price) / price >= ev/100.
    Rearranged: price <= fair / (1 + ev/100). Always round down to integer."""
    if fair_cents <= 0 or min_ev_pct <= 0:
        return max(1, fair_cents - 1)
    price = fair_cents / (1.0 + min_ev_pct / 100.0)
    return max(1, int(price))


def post_live_orders(tickers_list, home_fair_cents, contracts, spread_cents,
                     home_name='HOME', away_name='AWAY', trade_side='both'):
    """Post maker-only limit orders at target EV%. Caps price at best_ask - 1 to never cross."""
    away_fair_cents = 100 - home_fair_cents
    home_bid = _ev_bid(home_fair_cents, spread_cents)
    away_bid = _ev_bid(away_fair_cents, spread_cents)
    skip_home = home_bid < 1
    skip_away = away_bid < 1
    if trade_side == 'home':
        skip_away = True
    elif trade_side == 'away':
        skip_home = True

    orders_to_place = []
    for t in tickers_list:
        ticker = t['ticker']
        home_is_yes = t['home_is_yes']
        yes_bid = home_bid if home_is_yes else away_bid
        no_bid = away_bid if home_is_yes else home_bid
        yes_team = home_name if home_is_yes else away_name
        no_team = away_name if home_is_yes else home_name
        yes_fair = home_fair_cents if home_is_yes else away_fair_cents
        no_fair = away_fair_cents if home_is_yes else home_fair_cents
        yes_skip = skip_home if home_is_yes else skip_away
        no_skip = skip_away if home_is_yes else skip_home

        if yes_bid > yes_fair:
            yes_bid = yes_fair
        if no_bid > no_fair:
            no_bid = no_fair

        ob = _fetch_orderbook_prices(ticker)
        if ob:
            if ob['yes_best_ask'] is not None:
                maker_cap_yes = ob['yes_best_ask'] - 1
                if yes_bid >= ob['yes_best_ask']:
                    print(f"[MAKER] {yes_team} YES: capped {yes_bid}c -> {maker_cap_yes}c "
                          f"(ask={ob['yes_best_ask']}c)")
                    yes_bid = maker_cap_yes
            if ob['no_best_ask'] is not None:
                maker_cap_no = ob['no_best_ask'] - 1
                if no_bid >= ob['no_best_ask']:
                    print(f"[MAKER] {no_team} NO: capped {no_bid}c -> {maker_cap_no}c "
                          f"(ask={ob['no_best_ask']}c)")
                    no_bid = maker_cap_no

        if 1 <= yes_bid <= 99 and not yes_skip:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_bid,
                'team': f"{yes_team} YES", 'fair': yes_fair,
            })
        if 1 <= no_bid <= 99 and not no_skip:
            orders_to_place.append({
                'ticker': ticker, 'side': 'no', 'price': no_bid,
                'team': f"{no_team} NO", 'fair': no_fair,
            })

    home_ev = (home_fair_cents - home_bid) / home_bid * 100 if home_bid > 0 else 0
    away_ev = (away_fair_cents - away_bid) / away_bid * 100 if away_bid > 0 else 0
    print(f"[Post] Firing {len(orders_to_place)} orders | "
          f"{home_name} fair={home_fair_cents}c bid={home_bid}c ev={home_ev:.1f}% | "
          f"{away_name} fair={away_fair_cents}c bid={away_bid}c ev={away_ev:.1f}%")

    results = []
    def _place(spec):
        ok = place_tracked_order(spec['ticker'], spec['side'], contracts, spec['price'])
        return {
            'team': spec['team'], 'ticker': spec['ticker'],
            'kalshi_side': spec['side'], 'fair': spec['fair'],
            'price': spec['price'], 'contracts': contracts,
            'status': 'placed' if ok else 'failed',
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(_place, o) for o in orders_to_place]):
            results.append(f.result())
    return results


def _shrink_spread(spread_cents, fair_cents):
    if fair_cents >= 40:
        return spread_cents
    if fair_cents >= 30:
        return max(1, int(round(spread_cents * 0.75)))
    if fair_cents >= 20:
        return max(1, int(round(spread_cents * 0.55)))
    if fair_cents >= 10:
        return max(1, int(round(spread_cents * 0.35)))
    return max(1, int(round(spread_cents * 0.20)))


def post_ioc_orders(tickers_list, home_fair_cents, contracts, spread_cents,
                    home_name='HOME', away_name='AWAY', trade_side='both'):
    """Post IOC orders concurrently."""
    away_fair_cents = 100 - home_fair_cents
    home_spread = _shrink_spread(spread_cents, home_fair_cents)
    away_spread = _shrink_spread(spread_cents, away_fair_cents)
    skip_home = (home_fair_cents - home_spread) < 1
    skip_away = (away_fair_cents - away_spread) < 1
    if trade_side == 'home':
        skip_away = True
    elif trade_side == 'away':
        skip_home = True
    home_bid = max(1, home_fair_cents - home_spread)
    away_bid = max(1, away_fair_cents - away_spread)
    print(f"[IOC-SPREAD] fair={home_fair_cents}/{away_fair_cents}c "
          f"spread={home_spread}/{away_spread}c (base={spread_cents}c) "
          f"-> bid={home_bid}/{away_bid}c")

    orders_to_place = []
    for t in tickers_list:
        ticker = t['ticker']
        home_is_yes = t['home_is_yes']
        yes_bid = home_bid if home_is_yes else away_bid
        no_bid = away_bid if home_is_yes else home_bid
        yes_team = home_name if home_is_yes else away_name
        no_team = away_name if home_is_yes else home_name
        yes_fair = home_fair_cents if home_is_yes else away_fair_cents
        no_fair = away_fair_cents if home_is_yes else home_fair_cents
        yes_skip = skip_home if home_is_yes else skip_away
        no_skip = skip_away if home_is_yes else skip_home

        if yes_bid > yes_fair:
            yes_bid = yes_fair
        if no_bid > no_fair:
            no_bid = no_fair

        if 1 <= yes_bid <= 99 and not yes_skip:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_bid,
                'team': f"{yes_team} YES", 'fair': yes_fair,
            })
        if 1 <= no_bid <= 99 and not no_skip:
            orders_to_place.append({
                'ticker': ticker, 'side': 'no', 'price': no_bid,
                'team': f"{no_team} NO", 'fair': no_fair,
            })

    results = []
    def _take(spec):
        filled, total = place_ioc_order(spec['ticker'], spec['side'],
                                         contracts, spec['price'])
        return {
            'team': spec['team'], 'ticker': spec['ticker'],
            'kalshi_side': spec['side'], 'fair': spec['fair'],
            'price': spec['price'], 'contracts': contracts,
            'filled': filled,
            'status': 'filled' if filled > 0 else 'not_filled',
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(_take, o) for o in orders_to_place]):
            results.append(f.result())
    return results


def _fetch_orderbook_prices(ticker):
    """Fetch orderbook and return {yes_best_bid, yes_best_ask, no_best_bid, no_best_ask}."""
    try:
        url = f"{KALSHI_BASE}/markets/{ticker}/orderbook"
        resp = http_requests.get(url, timeout=5)
        if resp.status_code != 200:
            return None
        ob_json = resp.json()
        ob = ob_json.get('orderbook', {})
        yes_bids = ob.get('yes', [])
        no_bids = ob.get('no', [])
        if not yes_bids and not no_bids:
            ob_fp = ob_json.get('orderbook_fp', {})
            yes_fp = ob_fp.get('yes_dollars', [])
            no_fp = ob_fp.get('no_dollars', [])
            yes_bids = [[int(round(float(p) * 100)), int(float(q))] for p, q in yes_fp]
            no_bids = [[int(round(float(p) * 100)), int(float(q))] for p, q in no_fp]
        yes_bids.sort(key=lambda x: -x[0])
        no_bids.sort(key=lambda x: -x[0])
        # Yes ask = cheapest offer to sell YES = 100 - highest NO bid
        yes_best_ask = (100 - no_bids[0][0]) if no_bids else None
        no_best_ask = (100 - yes_bids[0][0]) if yes_bids else None
        return {
            'yes_best_bid': yes_bids[0][0] if yes_bids else None,
            'yes_best_ask': yes_best_ask,
            'no_best_bid': no_bids[0][0] if no_bids else None,
            'no_best_ask': no_best_ask,
        }
    except Exception:
        return None


def _fetch_best_bid(ticker, side):
    ob = _fetch_orderbook_prices(ticker)
    if not ob:
        return None
    return ob.get(f'{side}_best_bid')


# ── HLTV Scoreboard Scraper ──────────────────────────────────────────

HLTV_BASE = 'https://www.hltv.org'
HLTV_SCRAPER = None
LIVE_TRADER_CDP_PORT = 9223

_lt_chrome_proc = None
_lt_lock = threading.Lock()


def _ensure_lt_chrome():
    """Launch a dedicated Chrome debug instance on port 9223 if not already running."""
    global _lt_chrome_proc
    import subprocess, shutil, urllib.request

    with _lt_lock:
        # Check if already responding
        try:
            urllib.request.urlopen(f'http://localhost:{LIVE_TRADER_CDP_PORT}/json/version', timeout=2)
            return
        except Exception:
            pass

        if _lt_chrome_proc is not None and _lt_chrome_proc.poll() is None:
            time.sleep(1)
            return

        chrome_paths = [
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            shutil.which('google-chrome') or '',
            shutil.which('google-chrome-stable') or '',
        ]
        chrome_bin = next((p for p in chrome_paths if p and os.path.exists(p)), None)
        if not chrome_bin:
            raise RuntimeError("Chrome not found")
        _lt_chrome_proc = subprocess.Popen([
            chrome_bin,
            f'--remote-debugging-port={LIVE_TRADER_CDP_PORT}',
            '--no-first-run',
            '--no-default-browser-check',
            f'--user-data-dir={os.path.expanduser("~/.chrome-live-trader")}',
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Wait for debug port to become available
        for _ in range(10):
            time.sleep(1)
            try:
                urllib.request.urlopen(f'http://localhost:{LIVE_TRADER_CDP_PORT}/json/version', timeout=2)
                break
            except Exception:
                continue
        print(f"[HLTV] Launched Chrome debug on port {LIVE_TRADER_CDP_PORT}")


def _cdp_get_tabs():
    """Get list of open tabs from Chrome debug protocol (no Playwright needed)."""
    import urllib.request, json
    resp = urllib.request.urlopen(f'http://localhost:{LIVE_TRADER_CDP_PORT}/json', timeout=5)
    return json.loads(resp.read())


def _cdp_find_tab(url_fragment):
    """Find an open tab whose URL contains url_fragment. Returns the tab info dict or None."""
    try:
        for tab in _cdp_get_tabs():
            if tab.get('type') == 'page' and url_fragment in (tab.get('url') or ''):
                return tab
    except Exception:
        pass
    return None


class HLTVScoreboard:
    """Background Playwright scraper that polls the HLTV scorebot DOM."""

    def __init__(self):
        self._state = None
        self._lock = threading.Lock()
        self._match_id = None
        self._running = False
        self._thread = None
        self._prev_timer = None
        self._timer_stall_count = 0
        self._bomb_locked = False
        self._prev_round = -1

    def watch(self, match_id, match_url):
        self.stop()
        self._match_id = match_id
        self._prev_timer = None
        self._timer_stall_count = 0
        self._bomb_locked = False
        self._prev_round = -1
        self._running = True
        self._thread = threading.Thread(
            target=self._scrape_loop, args=(match_url,), daemon=True)
        self._thread.start()

    def _scrape_loop(self, match_url):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("[HLTV] playwright not installed")
            return

        try:
            _ensure_lt_chrome()
        except Exception as e:
            print(f"[HLTV] Could not start Chrome debug: {e}")
            self._running = False
            return

        full_url = f'{HLTV_BASE}{match_url}'

        with sync_playwright() as pw:
            try:
                browser = pw.chromium.connect_over_cdp(f"http://localhost:{LIVE_TRADER_CDP_PORT}")
            except Exception as e:
                print(f"[HLTV] Could not connect to Chrome on port {LIVE_TRADER_CDP_PORT}: {e}")
                self._running = False
                return

            page = None
            opened_new = False

            # Find existing tab with this match
            for ctx in browser.contexts:
                for p in ctx.pages:
                    try:
                        if match_url in (p.url or ''):
                            page = p
                            print(f"[HLTV] Found existing tab: {p.url}")
                            break
                    except Exception:
                        continue
                if page:
                    break

            if page is None:
                ctx = browser.contexts[0] if browser.contexts else browser.new_context()
                page = ctx.new_page()
                opened_new = True
                print(f"[HLTV] Opening new tab for {full_url}")
                try:
                    page.goto(full_url, timeout=30000, wait_until='domcontentloaded')
                    for _ in range(30):
                        if 'just a moment' not in (page.title() or '').lower():
                            break
                        time.sleep(1)
                    time.sleep(3)
                except Exception as e:
                    print(f"[HLTV] Navigation error: {e}")
                    try:
                        page.close()
                    except Exception:
                        pass
                    self._running = False
                    return

            # Wait for scorebot to load and populate with player data
            print("[HLTV] Waiting for scorebot player data...")
            loaded = False
            for _ in range(20):
                try:
                    rows = page.query_selector_all('table.team tbody tr')
                    if rows:
                        print(f"[HLTV] Scorebot loaded — {len(rows)} player rows")
                        loaded = True
                        break
                except Exception:
                    pass
                time.sleep(1.5)

            if not loaded:
                sbe = page.query_selector('#scoreboardElement')
                if sbe:
                    print(f"[HLTV] Scorebot element found but no player rows after 30s")
                    print(f"[HLTV] Continuing anyway — data may populate during match")
                else:
                    print("[HLTV] No scorebot element found — stopping")
                    if opened_new:
                        try:
                            page.close()
                        except Exception:
                            pass
                    self._running = False
                    return

            while self._running:
                try:
                    state = self._parse(page)
                    timer_s = state.get('timer_seconds', -1)
                    cur_round = state.get('round', 0)
                    ct_alive = sum(1 for p in state.get('ct_players', []) if p.get('alive'))
                    t_alive = sum(1 for p in state.get('t_players', []) if p.get('alive'))
                    round_live = ct_alive > 0 and t_alive > 0

                    if cur_round != self._prev_round:
                        self._bomb_locked = False
                        self._timer_stall_count = 0
                    self._prev_round = cur_round

                    if self._bomb_locked:
                        state['bomb_planted'] = True
                        state['bomb_source'] = 'locked'
                    elif round_live and timer_s >= 0 and self._prev_timer is not None:
                        prev = self._prev_timer
                        if timer_s == prev:
                            self._timer_stall_count += 1
                        else:
                            self._timer_stall_count = 0

                        timer_jump = prev - timer_s > 30 and timer_s <= 45

                        if self._timer_stall_count >= 2 or timer_jump:
                            state['bomb_planted'] = True
                            state['bomb_source'] = 'timer_jump' if timer_jump else 'timer_stall'
                            self._bomb_locked = True
                    else:
                        self._timer_stall_count = 0

                    if state.get('bomb_planted') and not self._bomb_locked and round_live:
                        self._bomb_locked = True
                        state['bomb_source'] = state.get('bomb_source', 'css')

                    self._prev_timer = timer_s

                    with self._lock:
                        self._state = state
                except Exception as e:
                    import traceback
                    print(f"[HLTV] Parse error: {e}")
                    traceback.print_exc()
                time.sleep(0.8)

            if opened_new:
                try:
                    page.close()
                except Exception:
                    pass

    def _parse(self, page):
        # Single JS evaluate — much faster than many query_selector calls
        return page.evaluate("""() => {
            const s = {
                round: 0, map: '', timer: '', timer_seconds: -1,
                bomb_planted: false,
                ct_score: 0, t_score: 0,
                ct_team: '', t_team: '',
                ct_logo: '', t_logo: '',
                ct_players: [], t_players: [],
                maps: [],
            };
            const rt = document.querySelector('.currentRoundText');
            if (rt) {
                const parts = rt.innerText.trim().split(' - ');
                s.round = parseInt(parts[0]) || 0;
                if (parts.length > 1) s.map = parts[1].trim();
            }
            const ct = document.querySelector('.ctScore');
            const t = document.querySelector('.tScore');
            if (ct) s.ct_score = parseInt(ct.innerText) || 0;
            if (t) s.t_score = parseInt(t.innerText) || 0;
            const tmr = document.querySelector('.timeText span');
            if (tmr) {
                s.timer = tmr.innerText.trim();
                const tp = s.timer.match(/(\\d+):(\\d+)/);
                if (tp) s.timer_seconds = parseInt(tp[1]) * 60 + parseInt(tp[2]);
            }
            // Bomb planted detection — wrapped in try/catch so it can't kill the parse
            try {
                const isRedColor = (c) => {
                    if (!c) return false;
                    const m = c.match(/(\\d+),\\s*(\\d+),\\s*(\\d+)/);
                    if (!m) return false;
                    const r = parseInt(m[1]), g = parseInt(m[2]), b = parseInt(m[3]);
                    return r > 140 && g < 90 && b < 90 && r > g * 1.8;
                };
                const checkElRed = (el) => {
                    try {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        return isRedColor(style.backgroundColor) || isRedColor(style.color) ||
                               isRedColor(style.fill) || isRedColor(style.borderColor) ||
                               !!(el.className || '').match(/bomb.*planted|planted|bomb.*red/i);
                    } catch(e) { return false; }
                };
                document.querySelectorAll('img[src*="bomb"], img[src*="c4"], [class*="bomb"], [class*="Bomb"]').forEach(el => {
                    if (s.bomb_planted) return;
                    try {
                        if (checkElRed(el) || checkElRed(el.parentElement)) s.bomb_planted = true;
                    } catch(e) {}
                });
                const timeDiv = document.querySelector('.time');
                const topBar = document.querySelector('.topbarBg');
                if (checkElRed(timeDiv) || checkElRed(topBar) || checkElRed(tmr)) {
                    s.bomb_planted = true;
                }
                if (document.querySelector('.bomb-planted, .bombPlanted, .planted')) {
                    s.bomb_planted = true;
                }
            } catch(e) { /* bomb detection failed, rely on timer stall in Python */ }

            // Map scores — optional, wrapped so it can't break the parse
            try {
                document.querySelectorAll('.mapButton, .map-name-holder, [class*="mapChangeButton"]').forEach(btn => {
                    try {
                        const mapInfo = {name: '', team1_score: 0, team2_score: 0, is_live: false, is_done: false};
                        const nameEl = btn.querySelector('.mapName, .map-name, [class*="mapname"]');
                        if (nameEl) mapInfo.name = nameEl.innerText.trim();
                        else { const txt = btn.innerText.trim(); if (txt && txt.length < 30) mapInfo.name = txt.split('\\n')[0].trim(); }
                        const scores = btn.querySelectorAll('.results-team-score, .score, [class*="Score"]');
                        if (scores.length >= 2) { mapInfo.team1_score = parseInt(scores[0].innerText) || 0; mapInfo.team2_score = parseInt(scores[1].innerText) || 0; }
                        if ((btn.className || '').match(/active|live|selected|current/i)) mapInfo.is_live = true;
                        const ms1 = mapInfo.team1_score, ms2 = mapInfo.team2_score;
                        if (ms1 + ms2 > 0) {
                            if ((ms1 >= 13 && ms1 - ms2 >= 2) || (ms1 >= 13 && ms2 <= 12)) mapInfo.is_done = true;
                            if ((ms2 >= 13 && ms2 - ms1 >= 2) || (ms2 >= 13 && ms1 <= 12)) mapInfo.is_done = true;
                        }
                        if (mapInfo.name || ms1 + ms2 > 0) s.maps.push(mapInfo);
                    } catch(e) {}
                });
            } catch(e) {}

            // Round history — parse the game log icons
            s.round_history = [];
            try {
                document.querySelectorAll('.roundHistoryTeamRow, .round-history-team-row').forEach(teamRow => {
                    const teamClass = (teamRow.className || '');
                    // First row is typically team1 (top), second is team2 (bottom)
                    teamRow.querySelectorAll('.roundHistory .round-icon, .round-history-icon, [class*="roundIcon"], img[src*="icon"]').forEach(icon => {
                        // Already captured via other method below
                    });
                });
                // HLTV scorebot round history: .roundHistory contains round result icons
                // Each round has a div/img with class or src indicating: ct_win, t_win, bomb, defuse, etc.
                const roundCols = document.querySelectorAll('.roundHistory .col, .roundHistoryLine .col, [class*="roundHistory"] [class*="col"]');
                if (roundCols.length > 0) {
                    roundCols.forEach((col, idx) => {
                        const rnd = {round: idx + 1, winner: '', win_type: ''};
                        const img = col.querySelector('img');
                        const cls = (col.className || '') + ' ' + (col.innerHTML || '');
                        if (img) {
                            const src = (img.src || img.getAttribute('src') || '').toLowerCase();
                            if (src.includes('bomb_explode') || src.includes('t_bomb')) { rnd.winner = 't'; rnd.win_type = 'bomb'; }
                            else if (src.includes('bomb_defuse') || src.includes('ct_defuse')) { rnd.winner = 'ct'; rnd.win_type = 'defuse'; }
                            else if (src.includes('ct_icon') || src.includes('ct_win') || src.includes('icon_ct')) { rnd.winner = 'ct'; rnd.win_type = 'elim'; }
                            else if (src.includes('t_icon') || src.includes('t_win') || src.includes('icon_t')) { rnd.winner = 't'; rnd.win_type = 'elim'; }
                            else if (src.includes('stopwatch') || src.includes('timeout') || src.includes('timer')) { rnd.winner = 'ct'; rnd.win_type = 'time'; }
                        }
                        if (!rnd.winner) {
                            const lc = cls.toLowerCase();
                            if (lc.includes('ct')) rnd.winner = 'ct';
                            else if (lc.includes('t_') || lc.match(/\\bt\\b/)) rnd.winner = 't';
                            if (lc.includes('bomb') && !lc.includes('defuse')) rnd.win_type = 'bomb';
                            else if (lc.includes('defuse')) rnd.win_type = 'defuse';
                            else if (lc.includes('time') || lc.includes('stopwatch')) rnd.win_type = 'time';
                            else rnd.win_type = 'elim';
                        }
                        if (rnd.winner) s.round_history.push(rnd);
                    });
                }
                // Fallback: parse from the round-history-line bars (each half has a row per team)
                if (s.round_history.length === 0) {
                    const lines = document.querySelectorAll('.round-history-line, .roundHistoryLine');
                    lines.forEach(line => {
                        line.querySelectorAll('img').forEach((img, idx) => {
                            const src = (img.src || '').toLowerCase();
                            const title = (img.title || img.alt || '').toLowerCase();
                            const rnd = {round: s.round_history.length + 1, winner: '', win_type: ''};
                            if (src.includes('bomb_explode') || title.includes('bomb')) { rnd.winner = 't'; rnd.win_type = 'bomb'; }
                            else if (src.includes('bomb_defuse') || title.includes('defuse')) { rnd.winner = 'ct'; rnd.win_type = 'defuse'; }
                            else if (src.includes('ct') || title.includes('ct')) { rnd.winner = 'ct'; rnd.win_type = 'elim'; }
                            else if (src.includes('t_') || title.includes('terrorist')) { rnd.winner = 't'; rnd.win_type = 'elim'; }
                            else if (src.includes('stopwatch') || title.includes('time')) { rnd.winner = 'ct'; rnd.win_type = 'time'; }
                            if (rnd.winner) s.round_history.push(rnd);
                        });
                    });
                }
            } catch(e) {}

            const BASE = 'https://www.hltv.org';
            document.querySelectorAll('table.team').forEach(table => {
                const thead = table.querySelector('thead');
                if (!thead) return;
                const isCT = (thead.className || '').includes('ctTeamHeaderBg');
                const side = isCT ? 'ct' : 't';
                const tn = table.querySelector('.teamName');
                if (tn) {
                    s[side + '_team'] = tn.innerText.trim();
                    const logo = tn.querySelector('img');
                    if (logo) s[side + '_logo'] = logo.src || '';
                }
                const players = [];
                table.querySelectorAll('tbody tr').forEach(row => {
                    const cls = row.className || '';
                    const deadClass = cls.includes('playerDeadText') || cls.includes('dead') ||
                                      cls.includes('Dead');
                    let lowOpacity = false;
                    try { lowOpacity = parseFloat(window.getComputedStyle(row).opacity) < 0.5; } catch(e) {}
                    let alive = !deadClass && !lowOpacity;
                    const p = {name:'', alive, hp:0, weapon_img:'', weapon_name:'',
                               armor_img:'', has_defuse:false, money:0,
                               kills:0, assists:0, deaths:0, adr:0};
                    const nc = row.querySelector('.nameCell');
                    if (nc) p.name = nc.getAttribute('title') || nc.innerText.trim();
                    const wi = row.querySelector('.weaponCell img');
                    if (wi && wi.src && !wi.src.includes('blank')) {
                        p.weapon_img = wi.src.startsWith('http') ? wi.src : BASE + wi.src;
                        p.weapon_name = wi.src.split('/').pop().replace('.png','');
                    }
                    const hp = row.querySelector('.hp-text');
                    if (hp) p.hp = parseInt(hp.innerText) || 0;
                    const hpBar = row.querySelector('.hp-bar');
                    if (!hp && hpBar) {
                        const w = hpBar.style.width;
                        if (w) p.hp = parseInt(w) || 0;
                    }
                    // If HP is explicitly 0, player is dead regardless of class
                    if (p.hp === 0 && (hp || hpBar)) {
                        p.alive = false;
                        alive = false;
                    }
                    const ai = row.querySelector('.armorCell img');
                    if (ai && ai.src && !ai.src.includes('blank')) {
                        p.armor_img = ai.src.startsWith('http') ? ai.src : BASE + ai.src;
                    }
                    if (row.querySelector('.defuseKit img')) p.has_defuse = true;
                    const mc = row.querySelector('.moneyCell');
                    if (mc) p.money = parseInt(mc.innerText.replace(/[$,]/g,'')) || 0;
                    const kc = row.querySelector('.killCell');
                    if (kc) p.kills = parseInt(kc.innerText) || 0;
                    const ac = row.querySelector('.assistCell');
                    if (ac) p.assists = parseInt(ac.innerText) || 0;
                    const dc = row.querySelector('.deathCell');
                    if (dc) p.deaths = parseInt(dc.innerText) || 0;
                    const adr = row.querySelector('.adrCell');
                    if (adr) p.adr = parseFloat(adr.innerText) || 0;
                    players.push(p);
                });
                s[side + '_players'] = players;
            });
            return s;
        }""")

    def get_state(self):
        with self._lock:
            return self._state

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        with self._lock:
            self._state = None
        self._match_id = None

    @property
    def is_watching(self):
        return self._running and self._thread and self._thread.is_alive()


def fetch_hltv_live_matches():
    """Scrape HLTV /matches via the live-trader's own Chrome debug instance (port 9223).
    Uses raw CDP websocket to get page HTML — no Playwright needed (thread-safe for Flask)."""
    import json, urllib.request
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    try:
        _ensure_lt_chrome()

        # Find or create a /matches tab via CDP HTTP API
        tab = _cdp_find_tab('hltv.org/matches')
        if tab is None:
            # Open a new tab navigating to /matches
            encoded = urllib.request.quote(f'{HLTV_BASE}/matches', safe='')
            urllib.request.urlopen(
                f'http://localhost:{LIVE_TRADER_CDP_PORT}/json/new?{encoded}', timeout=10)
            time.sleep(5)
            tab = _cdp_find_tab('hltv.org/matches')
            if tab is None:
                print("[HLTV] Could not open /matches tab")
                return []

        # Use CDP websocket to get the page HTML
        import websocket
        ws_url = tab['webSocketDebuggerUrl']
        ws = websocket.create_connection(ws_url, timeout=15)
        try:
            # Reload the page to get fresh data
            ws.send(json.dumps({'id': 1, 'method': 'Page.reload'}))
            time.sleep(4)
            # Get the HTML
            ws.send(json.dumps({
                'id': 2, 'method': 'Runtime.evaluate',
                'params': {'expression': 'document.documentElement.outerHTML'}
            }))
            resp = json.loads(ws.recv())
            while resp.get('id') != 2:
                resp = json.loads(ws.recv())
            html = resp.get('result', {}).get('result', {}).get('value', '')
        finally:
            ws.close()

        if not html:
            print("[HLTV] Got empty page HTML")
            return []

        soup = BeautifulSoup(html, 'html.parser')

        live_section = soup.find('div', class_='liveMatches')
        if not live_section:
            return []

        matches = []
        for wrapper in live_section.find_all('div', class_='match-wrapper'):
            if wrapper.get('live') != 'true':
                continue
            match_id = wrapper.get('data-match-id', '')
            team_names = wrapper.find_all(class_='match-teamname')
            if len(team_names) < 2:
                continue
            link = wrapper.find('a')
            match_url = link.get('href', '') if link else ''
            meta = wrapper.find(class_='match-meta')
            format_text = meta.get_text(strip=True) if meta else 'bo3'
            matches.append({
                'match_id': match_id,
                'team1': team_names[0].get_text(strip=True),
                'team2': team_names[1].get_text(strip=True),
                'url': match_url,
                'format': format_text,
            })
        return matches
    except Exception as e:
        import traceback
        print(f"[HLTV] Live match scrape failed: {e}")
        traceback.print_exc()
        return []


# ── HTML Template ────────────────────────────────────────────────────

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>CS2 Live Trader</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0a0a0a; color: #e0e0e0; padding: 12px 20px; }
h1 { color: #4fc3f7; font-size: 20px; }
h2 { color: #81c784; margin: 10px 0 6px; font-size: 16px; }

.container { max-width: 960px; margin: 0 auto; }

.tab-bar { display: flex; gap: 4px; margin-bottom: 12px; }
.tab { padding: 8px 20px; font-size: 14px; font-weight: 600; background: #1a1a2e;
    color: #888; border: 1px solid #333; border-radius: 6px 6px 0 0; cursor: pointer;
    transition: all 0.2s; }
.tab:hover { color: #e0e0e0; }
.tab.active { background: #0a0a0a; color: #4fc3f7; border-bottom-color: #0a0a0a; }

.top-bar { display: flex; align-items: center; gap: 15px; margin-bottom: 10px; flex-wrap: wrap; }
.top-bar select { padding: 6px 10px; font-size: 13px; background: #1a1a2e;
    color: #e0e0e0; border: 1px solid #333; border-radius: 4px; min-width: 340px; }
.pregame-chip { display: inline-block; background: #1a1a2e; border: 1px solid #333;
    border-radius: 4px; padding: 4px 8px; font-size: 12px; color: #aaa; }
.pregame-chip span { color: #fff; font-weight: 500; }

.map-tracker { display: flex; gap: 8px; justify-content: center; margin: 12px 0;
    align-items: center; }
.map-slot { display: flex; flex-direction: column; align-items: center; gap: 4px;
    background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 10px 18px;
    min-width: 90px; cursor: pointer; transition: all 0.2s; user-select: none; }
.map-slot:hover { border-color: #4fc3f7; }
.map-slot.home-won { background: #1b3a1b; border-color: #4caf50; }
.map-slot.away-won { background: #3a1b1b; border-color: #ef5350; }
.map-slot .map-num { font-size: 14px; font-weight: 600; }
.map-slot .map-result { font-size: 11px; font-weight: 600; min-height: 14px; }

.score-display { display: flex; gap: 20px; align-items: center; justify-content: center;
    margin: 12px 0; }
.score-team { text-align: center; }
.score-team .team-name { font-size: 14px; color: #aaa; margin-bottom: 4px; }
.score-team .score { font-size: 36px; font-weight: bold; }
.score-team .score.leading { color: #4fc3f7; }
.score-divider { font-size: 28px; color: #555; }

.round-section { display: flex; gap: 10px; justify-content: center; align-items: center;
    margin: 10px 0; padding: 10px; background: #111; border: 1px solid #333;
    border-radius: 8px; }
.round-label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px;
    margin-right: 6px; }
.round-adj { background: #1a1a2e; border: 1px solid #444; border-radius: 4px;
    color: #e0e0e0; font-size: 16px; cursor: pointer; padding: 4px 10px; user-select: none; }
.round-adj:hover { border-color: #4fc3f7; color: #4fc3f7; }
.round-val { font-size: 24px; font-weight: bold; min-width: 30px; text-align: center; }
.round-team-label { font-size: 11px; color: #666; max-width: 80px; text-align: center;
    overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }

.controls-row { display: flex; gap: 12px; align-items: center; justify-content: center;
    margin: 8px 0; flex-wrap: wrap; }
.input-group { display: flex; flex-direction: column; }
.input-group label { margin-bottom: 3px; color: #aaa; font-size: 11px;
    text-transform: uppercase; letter-spacing: 1px; }
.input-group input, .input-group select { padding: 7px; font-size: 15px;
    background: #1a1a2e; color: #fff; border: 1px solid #333; border-radius: 4px;
    width: 80px; text-align: center; }

.btn-row { display: flex; gap: 8px; margin: 10px 0; justify-content: center; flex-wrap: wrap; }
.btn { padding: 10px 18px; font-size: 13px; font-weight: 600;
    border: none; border-radius: 6px; cursor: pointer; transition: all 0.2s; }
.btn:hover { transform: translateY(-1px); }
.btn-compute { background: #b39ddb; color: #000; }
.btn-primary { background: #4fc3f7; color: #000; }
.btn-danger { background: #ef5350; color: #fff; }
.btn-cancel-post { background: #ff9800; color: #000; }
.btn-ioc { background: #ff8a65; color: #000; }

.results { background: #1a1a2e; border-radius: 8px; padding: 15px;
    margin-top: 10px; border: 1px solid #333; }
.fair-price { font-size: 28px; font-weight: bold; text-align: center; margin: 8px 0; }
.fair-price .home { color: #4fc3f7; }
.fair-price .away { color: #ff8a65; }
.fair-price .sep { color: #555; margin: 0 10px; }

.market-info { display: flex; justify-content: space-between;
    background: #111; padding: 8px 12px; border-radius: 6px;
    margin: 6px 0; font-size: 12px; color: #888; }

.scoreline-grid { display: flex; gap: 8px; flex-wrap: wrap; justify-content: center;
    margin-top: 10px; }
.scoreline-chip { background: #1a1a2e; border: 1px solid #333; border-radius: 6px;
    padding: 8px 14px; text-align: center; min-width: 80px; }
.scoreline-chip .sl-winner { font-size: 10px; color: #aaa; }
.scoreline-chip .sl-score { font-size: 16px; font-weight: 700; color: #fff; }
.scoreline-chip .sl-pct { font-size: 12px; color: #81c784; margin-top: 2px; }
.scoreline-chip .sl-fair { font-size: 11px; color: #4fc3f7; }

.ou-row { display: flex; gap: 8px; justify-content: center; margin-top: 8px; }
.ou-chip { background: #1a1a2e; border: 1px solid #333; border-radius: 6px;
    padding: 8px 18px; text-align: center; min-width: 100px; }
.ou-chip .ou-label { font-size: 10px; color: #aaa; text-transform: uppercase; }
.ou-chip .ou-pct { font-size: 16px; font-weight: 700; color: #fff; margin-top: 2px; }
.ou-chip .ou-fair { font-size: 11px; color: #4fc3f7; }

.orders-table { width: 100%; border-collapse: collapse; margin-top: 10px; }
.orders-table th { text-align: left; color: #aaa; padding: 6px;
    border-bottom: 1px solid #333; font-size: 11px; text-transform: uppercase; }
.orders-table td { padding: 6px; border-bottom: 1px solid #222; font-size: 13px; }
.status-placed { color: #81c784; }
.status-failed { color: #ef5350; }

.alert { padding: 10px 14px; border-radius: 6px; margin: 8px 0;
    font-weight: 500; font-size: 13px; }
.alert-success { background: #1b5e20; color: #a5d6a7; border: 1px solid #2e7d32; }
.alert-error { background: #b71c1c; color: #ef9a9a; border: 1px solid #c62828; }
.alert-info { background: #0d47a1; color: #90caf9; border: 1px solid #1565c0; }

.kbd-help { background: #111; border: 1px solid #333; border-radius: 4px;
    padding: 6px 10px; margin: 6px 0; font-size: 11px; color: #888; text-align: center; }
.kbd-help kbd { background: #222; border: 1px solid #444; border-radius: 3px;
    padding: 1px 4px; font-family: monospace; color: #ccc; font-size: 11px; }

#loading { display: none; text-align: center; padding: 10px; color: #4fc3f7; }
.spinner { display: inline-block; width: 18px; height: 18px;
    border: 3px solid #333; border-top-color: #4fc3f7;
    border-radius: 50%; animation: spin 0.8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

.dry-run-banner { background: #ff9800; color: #000; text-align: center;
    padding: 6px; font-weight: bold; border-radius: 4px; margin-bottom: 8px; font-size: 13px; }

.ob-panel { background: #111; border: 1px solid #333; border-radius: 6px; padding: 10px;
    margin-top: 10px; }
.ob-panel h3 { color: #4fc3f7; font-size: 13px; margin-bottom: 6px; text-align: center; }
.ob-book { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
.ob-side-header { font-size: 10px; text-transform: uppercase; letter-spacing: 1px;
    padding: 3px 6px; border-bottom: 1px solid #333; margin-bottom: 3px;
    display: flex; justify-content: space-between; }
.ob-side-header.bids { color: #81c784; }
.ob-side-header.asks { color: #ef5350; }
.ob-row { display: flex; justify-content: space-between; padding: 2px 6px; font-size: 12px;
    font-family: 'SF Mono', 'Menlo', monospace; position: relative; }
.ob-row .price { z-index: 1; }
.ob-row .qty { z-index: 1; color: #aaa; }
.ob-row.bid-row { color: #81c784; }
.ob-row.ask-row { color: #ef5350; }
.ob-row .depth-bar { position: absolute; top: 0; bottom: 0; opacity: 0.1; z-index: 0; }
.ob-row.bid-row .depth-bar { background: #81c784; right: 0; }
.ob-row.ask-row .depth-bar { background: #ef5350; left: 0; }
.ob-row.mine { background: rgba(79, 195, 247, 0.15); border-left: 3px solid #4fc3f7; }
.ob-row.mine .price::after { content: ' \\2605'; color: #4fc3f7; font-size: 10px; }
.ob-spread { text-align: center; padding: 4px; font-size: 11px; color: #888;
    border-top: 1px solid #222; border-bottom: 1px solid #222; margin: 3px 0; }

/* ── HLTV Scoreboard ─────────────────────────────── */
.sb-section { margin: 14px 0; }
.sb-controls { display: flex; gap: 10px; align-items: center; margin-bottom: 8px; flex-wrap: wrap; }
.sb-controls select { padding: 6px 10px; font-size: 13px; background: #1a1a2e;
    color: #e0e0e0; border: 1px solid #333; border-radius: 4px; min-width: 300px; }
.sb-controls .btn { padding: 6px 14px; font-size: 12px; }
.sb-status { font-size: 11px; color: #666; margin-left: 8px; }
.sb-status.connected { color: #81c784; }

.sb-board { background: #111; border: 1px solid #333; border-radius: 8px;
    overflow: hidden; margin-top: 6px; }
.sb-header { display: flex; justify-content: space-between; align-items: center;
    padding: 8px 14px; font-size: 13px; }
.sb-header .sb-map-info { color: #aaa; font-size: 12px; }
.sb-header .sb-timer { color: #ff9800; font-weight: 600; font-size: 14px; }
.sb-header .sb-round-num { color: #4fc3f7; font-weight: 600; }

.sb-team-header { display: flex; justify-content: space-between; align-items: center;
    padding: 6px 14px; font-size: 14px; font-weight: 600; }
.sb-team-header.ct { background: rgba(91, 155, 213, 0.15); color: #5B9BD5;
    border-top: 2px solid #5B9BD5; }
.sb-team-header.t { background: rgba(212, 168, 75, 0.15); color: #D4A84B;
    border-top: 2px solid #D4A84B; }
.sb-team-header .sb-team-score { font-size: 20px; }

.sb-players { width: 100%; }
.sb-player-row { display: grid;
    grid-template-columns: 36px 120px 80px 24px 24px 70px 36px 36px 36px 50px;
    align-items: center; padding: 3px 14px; font-size: 12px; border-bottom: 1px solid #1a1a1a;
    gap: 4px; }
.sb-player-row:last-child { border-bottom: none; }
.sb-player-row.dead { opacity: 0.35; }

.sb-weapon-icon { height: 18px; max-width: 34px; object-fit: contain; filter: brightness(0.9); }
.sb-player-name { white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    font-weight: 500; }

.sb-hp-bar-outer { width: 74px; height: 6px; background: #222; border-radius: 3px;
    overflow: hidden; position: relative; }
.sb-hp-bar-inner { height: 100%; border-radius: 3px; transition: width 0.3s; }
.sb-hp-bar-inner.hp-high { background: #4caf50; }
.sb-hp-bar-inner.hp-mid { background: #ff9800; }
.sb-hp-bar-inner.hp-low { background: #ef5350; }
.sb-hp-text { font-size: 10px; color: #888; text-align: right; width: 24px; display: inline-block; }

.sb-armor-icon, .sb-defuse-icon { height: 16px; width: 16px; object-fit: contain;
    filter: brightness(0.85); }
.sb-money { font-size: 11px; color: #81c784; font-family: 'SF Mono', 'Menlo', monospace;
    text-align: right; }
.sb-stat { font-size: 11px; color: #aaa; text-align: center;
    font-family: 'SF Mono', 'Menlo', monospace; }
.sb-stat.kills { color: #e0e0e0; font-weight: 600; }
.sb-stat.deaths { color: #ef5350; }

.sb-player-header { display: grid;
    grid-template-columns: 36px 120px 80px 24px 24px 70px 36px 36px 36px 50px;
    padding: 2px 14px; font-size: 10px; color: #555; text-transform: uppercase;
    letter-spacing: 0.5px; gap: 4px; border-bottom: 1px solid #222; }

.brk-toolbar { display: flex; gap: 8px; margin-bottom: 12px; align-items: center; flex-wrap: wrap; }
.brk-rounds { display: flex; gap: 20px; overflow-x: auto; padding: 10px 0; align-items: stretch; }
.brk-round { display: flex; flex-direction: column; justify-content: space-around;
    min-width: 200px; }
.brk-round-hdr { display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 6px; }
.brk-round-hdr .round-label { color: #4fc3f7; font-size: 11px; text-transform: uppercase;
    letter-spacing: 1px; cursor: pointer; }
.brk-round-hdr .round-actions { display: flex; gap: 4px; }
.brk-round-hdr .round-action { background: none; border: 1px solid #444; border-radius: 3px;
    color: #888; cursor: pointer; padding: 1px 5px; font-size: 10px; }
.brk-round-hdr .round-action:hover { color: #fff; border-color: #666; }
.brk-round-hdr .round-action.danger:hover { color: #ef5350; border-color: #ef5350; }

.brk-game { background: #1a1a2e; border: 1px solid #333; border-radius: 6px; margin: 4px 0;
    padding: 6px; position: relative; }
.brk-game-num { position: absolute; top: -8px; left: 8px; background: #0a0a0a;
    padding: 0 4px; font-size: 9px; color: #666; display: flex; align-items: center; }
.brk-slot { padding: 4px 6px; border-bottom: 1px solid #222; }
.brk-slot:last-child { border-bottom: none; }
.brk-slot select { width: 100%; padding: 4px 6px; font-size: 12px; background: #111;
    color: #e0e0e0; border: 1px solid #333; border-radius: 3px; }
.brk-slot select option.opt-team { color: #e0e0e0; }
.brk-slot select option.opt-winner { color: #b39ddb; }
.brk-game-connector { position: absolute; right: -20px; top: 50%; width: 20px; height: 1px;
    background: #333; }

.brk-add-round { min-width: 100px; display: flex; flex-direction: column; justify-content: center;
    align-items: center; }
.brk-add-round button { background: #1a1a2e; border: 1px dashed #444; border-radius: 8px;
    color: #888; padding: 20px 16px; cursor: pointer; font-size: 13px; }
.brk-add-round button:hover { border-color: #4fc3f7; color: #4fc3f7; }

.brk-results-table { width: 100%; border-collapse: collapse; margin-top: 16px; }
.brk-results-table th { text-align: left; color: #aaa; padding: 8px 10px; font-size: 11px;
    border-bottom: 1px solid #333; text-transform: uppercase; letter-spacing: 1px; }
.brk-results-table td { padding: 8px 10px; font-size: 13px; border-bottom: 1px solid #222; }
.brk-results-table tr:hover { background: #1a1a2e; }
.brk-results-table .team-name { font-weight: 600; color: #e0e0e0; }
.brk-results-table .pct { font-family: 'SF Mono', 'Menlo', monospace; color: #81c784; }
.brk-results-table .pct-bye { font-family: 'SF Mono', 'Menlo', monospace; color: #555; }
.brk-results-table .pct-high { font-family: 'SF Mono', 'Menlo', monospace;
    color: #4fc3f7; font-weight: 700; }
.brk-champ-bar { height: 8px; background: #222; border-radius: 4px; margin-top: 2px; }
.brk-champ-bar-fill { height: 100%; border-radius: 4px;
    background: linear-gradient(90deg, #4fc3f7, #81c784); }

.futures-event-row:hover { border-color: #4fc3f7 !important; }
.swiss-panel { padding: 10px 0; }
.swiss-stage { background: #111; border: 1px solid #333; border-radius: 8px; padding: 14px; margin-bottom: 14px; }
.swiss-stage-hdr { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
.swiss-stage-hdr .stage-label { color: #4fc3f7; font-size: 13px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1px; cursor: pointer; }
.swiss-stage-hdr .stage-label:hover { text-decoration: underline; }
.swiss-cfg { display: flex; gap: 14px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 10px; }
.swiss-cfg label { color: #aaa; font-size: 10px; text-transform: uppercase; display: block; margin-bottom: 2px; }
.swiss-cfg select, .swiss-cfg input[type=number] { background: #1a1a2e; color: #e0e0e0; border: 1px solid #333;
    border-radius: 4px; padding: 4px 8px; font-size: 12px; }
.swiss-team-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 12px; margin-top: 6px; }
.swiss-team-slot { display: flex; align-items: center; gap: 6px; padding: 3px 0; }
.swiss-team-slot .seed-num { color: #555; font-size: 11px; min-width: 24px; text-align: right; }
.swiss-team-slot select { flex: 1; background: #1a1a2e; color: #e0e0e0; border: 1px solid #333;
    border-radius: 4px; padding: 3px 6px; font-size: 12px; }
.swiss-team-slot .adv-tag { flex: 1; background: #1a2e1a; color: #81c784; border: 1px solid #2e7d32;
    border-radius: 4px; padding: 3px 8px; font-size: 12px; display: flex; align-items: center; justify-content: space-between; }
.swiss-team-slot .adv-tag .remove-adv { background: none; border: none; color: #ef5350; cursor: pointer;
    font-size: 14px; padding: 0 4px; }
.swiss-add-adv { display: flex; gap: 8px; align-items: center; margin-top: 8px; }
.swiss-add-adv select, .swiss-add-adv button { font-size: 11px; }
.swiss-record-bar { height: 6px; background: #222; border-radius: 3px; margin-top: 2px; }
.swiss-record-fill { height: 100%; border-radius: 3px; }
.swiss-record-fill.adv { background: linear-gradient(90deg, #4fc3f7, #81c784); }
.swiss-record-fill.elim { background: linear-gradient(90deg, #ef5350, #ff8a65); }
</style>
</head>
<body>
<div class="container">
    {% if dry_run %}
    <div class="dry-run-banner">DRY RUN MODE — No real orders will be placed</div>
    {% endif %}

    <div class="tab-bar">
        <button class="tab active" onclick="switchTab('trade')">Trade</button>
        <button class="tab" onclick="switchTab('esports')">Esports</button>
        <button class="tab" onclick="switchTab('ou-trade')">O/U Trade</button>
        <button class="tab" onclick="switchTab('predict')">Predict</button>
        <button class="tab" onclick="switchTab('bracket')">Bracket Builder</button>
        <button class="tab" onclick="switchTab('futures')">Futures</button>
        <button class="tab" onclick="switchTab('forfeit')">Forfeit</button>
        <button class="tab" onclick="switchTab('screen')">Screen Track</button>
    </div>

    <div id="tab-esports" style="display:none;">
        <h2 style="color:#e040fb; margin-bottom:8px;">Esports Screen Trader</h2>
        <p style="font-size:12px; color:#888; margin-bottom:10px;">
            Track live odds via screen capture for Valorant, LoL, and Dota 2 matches. Add games, mark odds regions, and trade.
        </p>

        <div style="display:flex; gap:8px; align-items:flex-end; margin-bottom:10px; flex-wrap:wrap;">
            <div class="input-group">
                <label style="font-size:11px;">Esport</label>
                <select id="esp-esport" onchange="espOnEsportChange()" style="width:160px;padding:5px 8px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
                    <option value="">-- Select --</option>
                    <option value="valorant">Valorant</option>
                    <option value="lol">League of Legends</option>
                    <option value="dota2">Dota 2</option>
                </select>
            </div>
            <div class="input-group">
                <label style="font-size:11px;">Match</label>
                <select id="esp-game-select" style="min-width:300px;padding:5px 8px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
                    <option value="">-- Pick esport first --</option>
                </select>
            </div>
            <button class="btn" onclick="espAddSelectedGame()" style="padding:5px 10px;font-size:11px;background:#7b1fa2;color:#e040fb;border:1px solid #e040fb;font-weight:bold;">Add Game</button>
            <span id="esp-fetch-status" style="font-size:11px;color:#666;"></span>
        </div>

        <div style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">
            <button class="btn" id="esp-trade-btn" onclick="espToggleTrade()"
                style="background:#1b5e20;color:#66bb6a;border-color:#66bb6a;font-weight:bold;padding:5px 14px;font-size:12px;">TRADE ON</button>
            <button class="btn" id="esp-mode-btn" onclick="espToggleMode()"
                style="background:#333;color:#ffa726;border-color:#ffa726;font-weight:bold;font-size:11px;padding:5px 10px;">IOC</button>
            <div class="input-group" style="margin-left:10px;">
                <label style="font-size:10px;">Contracts</label>
                <input id="esp-contracts" type="number" value="50" min="1" max="999" style="width:60px;padding:4px 6px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
            </div>
            <div class="input-group">
                <label style="font-size:10px;">Spread</label>
                <input id="esp-spread" type="number" value="6" min="1" max="20" style="width:50px;padding:4px 6px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
            </div>
            <span id="esp-ws-status" style="font-size:10px;font-weight:bold;color:#555;">WS:OFF</span>
        </div>

        <div id="esp-tracked-games" style="margin-bottom:8px;"></div>
        <div id="esp-trade-log" style="display:none;max-height:120px;overflow-y:auto;margin-bottom:8px;padding:4px 8px;background:#0a0a1a;border:1px solid #333;border-radius:4px;font-family:monospace;font-size:11px;color:#888;"></div>

        <div style="display:flex; gap:6px; align-items:center; margin-bottom:8px;">
            <button class="btn" id="esp-stream-toggle" onclick="espToggleStream()" style="padding:4px 12px;font-size:11px;background:#333;">Live View</button>
            <button class="btn esp-region-btn" id="esp-rgn-btn-scoreboard" onclick="espSetRegionMode('scoreboard')"
                    style="padding:4px 10px;font-size:11px;background:#333;border:1px solid #555;">Mark Odds Region</button>
            <button class="btn" onclick="espClearAll()" style="padding:4px 8px;font-size:11px;background:#333;color:#ef5350;">Clear All</button>
            <span id="esp-stream-fps" style="font-size:11px; color:#444;"></span>
        </div>
        <div id="esp-screenshot-container" style="position:relative; max-width:100%; overflow:hidden;
             border:1px solid #333; border-radius:4px; height:250px; background:#111; display:none;">
            <div id="esp-screen-inner" style="transform-origin:0 0; position:relative; width:100%;">
                <img id="esp-screen-img" style="display:none; width:100%;">
                <canvas id="esp-screen-canvas" style="position:absolute; top:0; left:0; width:100%; height:100%;"></canvas>
            </div>
        </div>
        <div id="esp-sb-section" style="display:none; margin-top:8px;">
            <div id="esp-mark-buttons" style="display:flex; gap:6px; margin-bottom:6px; align-items:center; flex-wrap:wrap;"></div>
            <div id="esp-sb-preview" style="position:relative; border:1px solid #e040fb; border-radius:4px;
                 background:#111; overflow:hidden; cursor:crosshair;">
                <img id="esp-sb-img" style="display:block; width:100%;">
                <canvas id="esp-sb-canvas" style="position:absolute; top:0; left:0; width:100%; height:100%;"></canvas>
            </div>
        </div>

        <div id="esp-history-section" style="margin-top:12px;">
            <h3 style="color:#aaa; font-size:13px; margin-bottom:6px;">Trade History</h3>
            <div id="esp-history-log" style="max-height:200px; overflow-y:auto; font-family:'SF Mono','Menlo',monospace;
                 font-size:12px; color:#aaa; background:#0a0a0a; border-radius:4px; padding:8px;"></div>
        </div>
    </div>

    <div id="tab-predict" style="display:none;">
        <h2>Match Projection</h2>
        <div style="display:flex; gap:12px; align-items:flex-end; margin:12px 0; flex-wrap:wrap;">
            <div class="input-group">
                <label>Team A</label>
                <input type="text" id="pred-team-a" list="team-list-a" placeholder="Search..." style="width:200px;">
                <datalist id="team-list-a"></datalist>
            </div>
            <span style="color:#555; font-size:20px; padding-bottom:6px;">vs</span>
            <div class="input-group">
                <label>Team B</label>
                <input type="text" id="pred-team-b" list="team-list-b" placeholder="Search..." style="width:200px;">
                <datalist id="team-list-b"></datalist>
            </div>
            <div class="input-group">
                <label>Format</label>
                <select id="pred-format" style="width:90px; padding:6px 8px; background:#1a1a2e; color:#e0e0e0; border:1px solid #333; border-radius:4px;">
                    <option value="bo1">BO1</option>
                    <option value="bo3" selected>BO3</option>
                    <option value="bo5">BO5</option>
                </select>
            </div>
            <button class="btn btn-compute" onclick="runPredict()">Predict</button>
        </div>
        <div id="pred-result" class="results" style="display:none;">
            <div class="fair-price">
                <span class="home" id="pred-a-name"></span>
                <span id="pred-a-pct"></span>
                <span class="sep">—</span>
                <span class="away" id="pred-b-name"></span>
                <span id="pred-b-pct"></span>
            </div>
            <div style="text-align:center; color:#aaa; font-size:14px; margin-top:6px; font-weight:600;">
                <span id="pred-a-ml"></span> / <span id="pred-b-ml"></span>
            </div>
            <div style="text-align:center; color:#666; font-size:12px; margin-top:4px;">
                <span id="pred-a-games"></span> · <span id="pred-b-games"></span>
            </div>
            <div id="pred-ou-row" class="ou-row" style="margin-top:10px;"></div>
        </div>
    </div>

    <div id="tab-bracket" style="display:none;">
        <h1 style="color: #4fc3f7; margin-bottom: 4px;">Bracket Builder</h1>
        <p style="font-size: 12px; color: #888; margin-bottom: 14px;">
            Build any bracket structure round-by-round. Supports byes, losers brackets, and custom formats.
        </p>

        <div class="brk-toolbar">
            <span style="color:#aaa; font-size:12px; text-transform:uppercase; letter-spacing:1px;">Quick Start:</span>
            <button class="btn btn-compute" onclick="brkPreset('4se')" style="padding:5px 12px;font-size:12px;">4-Team SE</button>
            <button class="btn btn-compute" onclick="brkPreset('8se')" style="padding:5px 12px;font-size:12px;">8-Team SE</button>
            <button class="btn btn-compute" onclick="brkPreset('8de')" style="padding:5px 12px;font-size:12px;">8-Team DE</button>
            <button class="btn btn-compute" onclick="brkPreset('12se4')" style="padding:5px 12px;font-size:12px;">12-Team (4 Byes)</button>
            <button class="btn btn-compute" onclick="brkPreset('swiss')" style="padding:5px 12px;font-size:12px;background:#1a2e1a;border-color:#2e7d32;">Swiss</button>
            <button class="btn" onclick="brkClear()" style="padding:5px 12px;font-size:12px;background:#333;color:#ccc;">Clear All</button>
            <span style="border-left:1px solid #444;margin:0 6px;"></span>
            <button class="btn" onclick="brkSave()" style="padding:5px 12px;font-size:12px;background:#1a1a2e;border-color:#4fc3f7;color:#4fc3f7;">Save</button>
            <select id="bracket-saves" onchange="brkLoad(this.value)" style="padding:5px 8px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
                <option value="">-- Saved Brackets --</option>
            </select>
            <button class="btn" onclick="brkDeleteSaved()" style="padding:5px 8px;font-size:12px;background:#333;color:#ff5252;" title="Delete selected">&#x2715;</button>
        </div>

        <div id="brk-editor">
            <div id="brk-bracket-area" class="brk-rounds"></div>
            <div id="swiss-panel" class="swiss-panel" style="display:none;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                    <span style="color:#81c784;font-size:12px;text-transform:uppercase;letter-spacing:1px;">Swiss Format</span>
                    <button class="btn btn-compute" onclick="swissAddStage()" style="padding:4px 12px;font-size:11px;background:#1a2e1a;border-color:#2e7d32;">+ Add Stage</button>
                </div>
                <div id="swiss-stages"></div>
            </div>
        </div>

        <div style="margin: 14px 0; display: flex; gap: 16px; align-items: flex-end; flex-wrap: wrap;">
            <div class="input-group" style="max-width: 160px;">
                <label>Simulations</label>
                <input type="number" id="bracket-sims" value="100000" min="1000" max="500000" style="width:120px;">
            </div>
            <button class="btn btn-primary" onclick="brkSimulate()" style="padding: 10px 24px;">
                Simulate Tournament
            </button>
        </div>

        <div id="bracket-loading" style="display: none; text-align: center; padding: 20px; color: #4fc3f7;">
            <div class="spinner"></div> Computing matchup probabilities &amp; running simulation...
        </div>

        <div id="bracket-results"></div>
    </div>

    <div id="tab-futures" style="display:none;">
        <h1 style="color: #4fc3f7; margin-bottom: 4px;">Futures / Props Trading</h1>
        <p style="font-size: 12px; color: #888; margin-bottom: 14px;">
            Type "kalshi" to load all CS2 futures, or paste a Polymarket event URL.
        </p>

        <div class="controls-row">
            <div class="input-group" style="flex:1;">
                <label>Event URL / Ticker</label>
                <input type="text" id="futures-url" placeholder="kalshi, https://polymarket.com/event/..., or KALSHI:event_ticker" style="width:100%;">
            </div>
            <button class="btn btn-compute" onclick="futuresFetchEvent()" style="margin-top:18px; padding:10px 20px;">
                Fetch Event
            </button>
            <button class="btn" onclick="futuresImportBracket()" style="margin-top:18px; padding:10px 16px; background:#1a2e1a; border-color:#2e7d32; color:#81c784;">
                Import from Bracket
            </button>
        </div>

        <div id="futures-event-title" style="display:none; margin:10px 0; font-size:14px; color:#4fc3f7; font-weight:600;"></div>

        <div id="futures-event-list" style="display:none; margin-top:16px;"></div>

        <div id="futures-loading" style="display:none; text-align:center; padding:20px; color:#4fc3f7;">
            <div class="spinner"></div> Fetching event markets...
        </div>

        <div id="futures-table-container" style="display:none; margin-top:16px;">
            <table class="brk-results-table" id="futures-table">
                <thead>
                    <tr>
                        <th style="width:30px;"><input type="checkbox" id="futures-check-all" checked onchange="futuresToggleAll(this)"></th>
                        <th>Team</th>
                        <th style="text-align:right;">Ask (Yes)</th>
                        <th style="text-align:right;">Ask (No)</th>
                        <th style="text-align:right; width:100px;">Fair (c)</th>
                        <th style="text-align:right;">Edge (Yes)</th>
                        <th style="text-align:right;">Edge (No)</th>
                        <th style="text-align:right;">Bid (Yes)</th>
                        <th style="text-align:right;">Bid (No)</th>
                    </tr>
                </thead>
                <tbody id="futures-tbody"></tbody>
            </table>

            <div class="controls-row" style="margin-top:14px;">
                <div class="input-group">
                    <label>Spread (c)</label>
                    <input type="number" id="futures-spread" value="{{ spread }}" min="1" max="50" style="width:80px;" onchange="futuresRenderTable()">
                </div>
                <div class="input-group">
                    <label>Shares/Contracts</label>
                    <input type="number" id="futures-size" value="50" min="5" max="1000" style="width:80px;">
                </div>
                <button class="btn btn-primary" onclick="futuresPostOrders()" style="margin-top:18px;">Post Orders</button>
                <button class="btn" onclick="futuresCancelAll()" style="margin-top:18px; background:#b71c1c; border-color:#ef5350;">Cancel All</button>
                <button class="btn" onclick="futuresRefreshAsks()" style="margin-top:18px; background:#1a1a2e; border-color:#4fc3f7; color:#4fc3f7;">Refresh Asks</button>
            </div>

            <div id="futures-status" style="margin-top:10px; font-size:13px; color:#888;"></div>
        </div>
    </div>

    <div id="match-state" style="display:block;">
    <div class="top-bar">
        <h1>CS2 Live Trader</h1>
        <select id="game-select" onchange="onGameSelect()">
            <option value="">-- Select a match --</option>
            {% for g in games %}
            <option value="{{ g.home }}|{{ g.away }}"
                data-home-prob="{{ g.home_prob }}"
                data-tickers='{{ g.tickers_json }}'
                data-ou-tickers='{{ g.ou_tickers_json }}'
                data-poly-series='{{ g.poly_series_json }}'>
                {{ g.away }} vs {{ g.home }} &mdash; {{ g.home }} {{ g.home_pct }}
            </option>
            {% endfor %}
        </select>
        <select id="best-of-select" onchange="onBestOfChange()" style="padding:6px 10px;font-size:13px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
            <option value="3" selected>BO3</option>
            <option value="1">BO1</option>
            <option value="5">BO5</option>
        </select>
        <div id="pregame-chip" class="pregame-chip" style="display:none;">
            Pregame: <span id="pg-prob"></span>
        </div>
    </div>

    <div id="match-section" style="display:none;">

        <div class="map-tracker" id="map-tracker">
            <div class="map-slot" data-map="1" onclick="cycleMap(1)">
                <div class="map-num">Map 1</div>
                <div class="map-result" id="map1-result">&mdash;</div>
            </div>
            <div class="map-slot" data-map="2" onclick="cycleMap(2)">
                <div class="map-num">Map 2</div>
                <div class="map-result" id="map2-result">&mdash;</div>
            </div>
            <div class="map-slot" data-map="3" onclick="cycleMap(3)">
                <div class="map-num">Map 3</div>
                <div class="map-result" id="map3-result">&mdash;</div>
            </div>
            <div class="map-slot" data-map="4" onclick="cycleMap(4)" style="display:none;">
                <div class="map-num">Map 4</div>
                <div class="map-result" id="map4-result">&mdash;</div>
            </div>
            <div class="map-slot" data-map="5" onclick="cycleMap(5)" style="display:none;">
                <div class="map-num">Map 5</div>
                <div class="map-result" id="map5-result">&mdash;</div>
            </div>
        </div>

        <div class="score-display">
            <div class="score-team">
                <div class="team-name" id="home-label">HOME</div>
                <div class="score" id="home-score">0</div>
            </div>
            <div class="score-divider">&mdash;</div>
            <div class="score-team">
                <div class="team-name" id="away-label">AWAY</div>
                <div class="score" id="away-score">0</div>
            </div>
        </div>

        <div class="round-section">
            <span class="round-label">Rounds</span>
            <span class="round-team-label" id="round-home-label">HOME</span>
            <button class="round-adj" onclick="adjRounds('home',-1)">&minus;</button>
            <span class="round-val" id="home-rounds">0</span>
            <button class="round-adj" onclick="adjRounds('home',1)">+</button>
            <span style="color:#555; font-size:20px; margin:0 8px;">:</span>
            <button class="round-adj" onclick="adjRounds('away',-1)">&minus;</button>
            <span class="round-val" id="away-rounds">0</span>
            <button class="round-adj" onclick="adjRounds('away',1)">+</button>
            <span class="round-team-label" id="round-away-label">AWAY</span>
        </div>

        <div class="alive-section" style="display:flex; align-items:center; gap:8px; margin:6px 0; flex-wrap:wrap;">
            <span style="font-size:12px; color:#888;">Alive</span>
            <span class="round-team-label" id="alive-home-label" style="font-size:12px;">HOME</span>
            <button class="round-adj" onclick="adjAlive('home',-1)" style="background:#b71c1c;">&darr;</button>
            <span class="round-val" id="home-alive" style="color:#4caf50; min-width:18px; text-align:center;">5</span>
            <button class="round-adj" onclick="adjAlive('home',1)" style="background:#1b5e20;">&uarr;</button>
            <span style="color:#555; font-size:16px; margin:0 4px;">v</span>
            <button class="round-adj" onclick="adjAlive('away',-1)" style="background:#b71c1c;">&darr;</button>
            <span class="round-val" id="away-alive" style="color:#ef5350; min-width:18px; text-align:center;">5</span>
            <button class="round-adj" onclick="adjAlive('away',1)" style="background:#1b5e20;">&uarr;</button>
            <span class="round-team-label" id="alive-away-label" style="font-size:12px;">AWAY</span>
            <button class="round-adj" onclick="resetAlive()" style="background:#333; color:#888; font-size:10px; padding:2px 8px;">5v5</button>
        </div>

        <div class="side-section" style="display:flex; align-items:center; gap:12px; margin:8px 0; flex-wrap:wrap;">
            <span style="font-size:12px; color:#888;">Map:</span>
            <select id="map-select" onchange="onMapSelect()" style="padding:4px 8px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
                <option value="">—</option>
                <option value="mirage">Mirage (55/45)</option>
                <option value="nuke">Nuke (57/43)</option>
                <option value="inferno">Inferno (53/47)</option>
                <option value="ancient">Ancient (54/46)</option>
                <option value="anubis">Anubis (52/48)</option>
                <option value="dust2">Dust2 (51/49)</option>
                <option value="vertigo">Vertigo (54/46)</option>
                <option value="train">Train (53/47)</option>
            </select>
            <span style="font-size:12px; color:#888;">CT:</span>
            <select id="ct-team-select" onchange="computeFair();ouComputeFair()" style="padding:4px 8px;font-size:12px;background:#1a1a2e;color:#4fc3f7;border:1px solid #333;border-radius:4px;">
                <option value="home" id="ct-opt-home">HOME</option>
                <option value="away" id="ct-opt-away">AWAY</option>
            </select>
            <span style="font-size:12px; color:#888;">CT%</span>
            <input type="number" id="ct-win-pct" placeholder="—" min="1" max="99" style="width:50px;padding:4px;font-size:12px;background:#1a1a2e;color:#4fc3f7;border:1px solid #333;border-radius:4px;text-align:center;" title="CT round win % (leave blank for model default)" onchange="computeFair();ouComputeFair()">
            <span style="font-size:12px; color:#888;">T%</span>
            <input type="number" id="t-win-pct" placeholder="—" min="1" max="99" style="width:50px;padding:4px;font-size:12px;background:#ef9a9a;border:1px solid #333;border-radius:4px;text-align:center;" title="T round win % (leave blank for model default)" onchange="computeFair();ouComputeFair()">
            <span style="font-size:11px; color:#555;" id="half-label"></span>
        </div>

        <div class="buy-section" style="display:flex; align-items:center; gap:10px; margin:4px 0; flex-wrap:wrap;">
            <span style="font-size:12px; color:#888;" id="home-buy-label">HOME:</span>
            <select id="home-buy" onchange="onBuyChange('home')" style="padding:4px 6px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
                <option value="">auto</option>
                <option value="full">Full</option>
                <option value="force">Force</option>
                <option value="eco">Eco</option>
            </select>
            <input type="number" id="home-money" placeholder="$" min="0" max="100000" style="width:62px;padding:4px;font-size:12px;background:#1a1a2e;color:#81c784;border:1px solid #333;border-radius:4px;text-align:center;" title="Team money sum (auto-sets buy type)" oninput="onMoneyInput('home')">
            <span style="color:#555; margin:0 2px;">|</span>
            <span style="font-size:12px; color:#888;" id="away-buy-label">AWAY:</span>
            <select id="away-buy" onchange="onBuyChange('away')" style="padding:4px 6px;font-size:12px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;">
                <option value="">auto</option>
                <option value="full">Full</option>
                <option value="force">Force</option>
                <option value="eco">Eco</option>
            </select>
            <input type="number" id="away-money" placeholder="$" min="0" max="100000" style="width:62px;padding:4px;font-size:12px;background:#1a1a2e;color:#ef9a9a;border:1px solid #333;border-radius:4px;text-align:center;" title="Team money sum (auto-sets buy type)" oninput="onMoneyInput('away')">
        </div>

        <div class="sb-section" id="sb-section" style="display:none;">
            <div class="sb-controls">
                <select id="hltv-select">
                    <option value="">-- HLTV Live Matches --</option>
                </select>
                <button class="btn btn-primary" onclick="watchHLTV()" style="padding:6px 14px;font-size:12px;">Watch</button>
                <button class="btn btn-danger" onclick="stopHLTV()" style="padding:6px 14px;font-size:12px;">Stop</button>
                <button class="btn btn-compute" onclick="refreshHLTVList()" style="padding:6px 14px;font-size:12px;">Refresh</button>
                <span class="sb-status" id="sb-status">Not connected</span>
            </div>
            <div class="sb-board" id="sb-board" style="display:none;"></div>
        </div>

    </div><!-- /match-section -->
    </div><!-- /match-state -->

    <div id="tab-trade">
    <div id="trade-match-content" style="display:none;">
        <div class="results" id="fair-section">
            <div class="fair-price">
                <span class="home" id="home-fair">&mdash;</span>
                <span class="sep">/</span>
                <span class="away" id="away-fair">&mdash;</span>
            </div>
            <div class="market-info">
                <span>Pregame: <span id="pregame-pct">&mdash;</span></span>
                <span>Map: <span id="map-pct">&mdash;</span></span>
                <span>Live: <span id="live-pct">&mdash;</span></span>
                <span>Score: <span id="score-label">0-0</span></span>
            </div>
            <div class="market-info" id="econ-info" style="display:none; margin-top:4px;">
                <span id="home-econ-label"></span>
                <span id="away-econ-label"></span>
                <span>Econ adj: <span id="econ-adj-pct">&mdash;</span></span>
            </div>
        </div>

        <div id="scoreline-section">
            <div id="scoreline-grid" class="scoreline-grid"></div>
            <div id="ou-row" class="ou-row"></div>
        </div>

        <div class="controls-row">
            <div class="input-group">
                <label>Contracts</label>
                <input type="number" id="contracts" value="{{ contracts }}" min="1" max="500">
            </div>
            <div class="input-group">
                <label>Spread (c)</label>
                <input type="number" id="spread-cents" value="{{ spread }}" min="1" max="50">
            </div>
            <div class="input-group">
                <label>Override %</label>
                <input type="number" id="override-prob" placeholder="&mdash;" min="1" max="99"
                    style="width:70px;" title="Override model probability (home team)">
            </div>
        </div>

        <div class="btn-row">
            <button class="btn btn-compute" onclick="computeFair()">Compute Fair</button>
            <button class="btn btn-primary" onclick="startAutoReprice()">Post Liquidity</button>
            <button class="btn btn-ioc" onclick="postIOC()">IOC Take</button>
            <button class="btn btn-cancel-post" onclick="cancelRepost()">Cancel &amp; Repost</button>
            <button class="btn btn-danger" onclick="cancelAll()">Cancel All</button>
            <button class="btn" id="auto-trade-btn" onclick="toggleAutoTrade()"
                style="background:#1b5e20;color:#66bb6a;border-color:#66bb6a;font-weight:bold;">AUTO TRADE</button>
            <button class="btn" id="screen-track-btn" onclick="toggleScreenTrack()"
                style="background:#1a237e;color:#7986cb;border-color:#7986cb;font-weight:bold;">SCREEN TRACK</button>
            <button class="btn" id="screen-trade-btn" onclick="toggleScreenTrade()" style="display:none;
                background:#1b5e20;color:#66bb6a;border-color:#66bb6a;font-weight:bold;">TRADE ON</button>
            <button class="btn" id="screen-mode-btn" onclick="toggleScreenTradeMode()" style="display:none;
                background:#333;color:#ffa726;border-color:#ffa726;font-weight:bold;font-size:11px;">IOC</button>
            <span id="ws-status" style="font-size:10px;font-weight:bold;margin-left:8px;color:#555;">WS:OFF</span>
        </div>

        <div class="kbd-help">
            <kbd>C</kbd> Compute &nbsp; <kbd>P</kbd> Auto-Reprice &nbsp;
            <kbd>I</kbd> IOC &nbsp; <kbd>R</kbd> Repost &nbsp; <kbd>X</kbd> Stop+Cancel &nbsp;
            <kbd>J</kbd> Jump &nbsp; <kbd>B</kbd> Buy &nbsp; <kbd>A</kbd> Auto Trade
            &nbsp; <span id="reprice-status"></span>
        </div>

        <div id="screen-trade-section" style="display:none; margin-top:14px; border:1px solid #1a237e; border-radius:6px; padding:12px;">
            <h3 style="color:#7986cb; margin:0 0 8px;">Screen Odds Tracker</h3>
            <div id="sct-tracked-games" style="margin-bottom:8px;"></div>
            <div id="sct-trade-log" style="display:none;max-height:80px;overflow-y:auto;margin-bottom:8px;padding:4px 8px;background:#0a0a1a;border:1px solid #333;border-radius:4px;font-family:monospace;font-size:11px;color:#888;"></div>
            <div style="display:flex; gap:6px; align-items:center; margin-bottom:8px;">
                <button class="btn" id="sct-stream-toggle" onclick="sctToggleStream()" style="padding:4px 12px;font-size:11px;background:#333;">Live View</button>
                <button class="btn sct-region-btn" id="sct-rgn-btn-scoreboard" onclick="sctSetRegionMode('scoreboard')"
                        style="padding:4px 10px;font-size:11px;background:#333;border:1px solid #555;">Mark Odds Region</button>
                <button class="btn" id="sct-add-game-btn" onclick="sctAddCurrentGame()"
                        style="padding:4px 10px;font-size:11px;background:#1a237e;color:#7986cb;border:1px solid #7986cb;">Add Game</button>
                <button class="btn" onclick="sctClearAll()" style="padding:4px 8px;font-size:11px;background:#333;color:#ef5350;">Clear All</button>
                <span id="sct-stream-fps" style="font-size:11px; color:#444;"></span>
            </div>
            <div id="sct-screenshot-container" style="position:relative; max-width:100%; overflow:hidden;
                 border:1px solid #333; border-radius:4px; height:250px; background:#111; display:none;">
                <div id="sct-screen-inner" style="transform-origin:0 0; position:relative; width:100%;">
                    <img id="sct-screen-img" style="display:none; width:100%;">
                    <canvas id="sct-screen-canvas" style="position:absolute; top:0; left:0; width:100%; height:100%;"></canvas>
                </div>
            </div>
            <div id="sct-sb-section" style="display:none; margin-top:8px;">
                <div id="sct-mark-buttons" style="display:flex; gap:6px; margin-bottom:6px; align-items:center; flex-wrap:wrap;"></div>
                <div id="sct-sb-preview" style="position:relative; border:1px solid #7986cb; border-radius:4px;
                     background:#111; overflow:hidden; cursor:crosshair;">
                    <img id="sct-sb-img" style="display:block; width:100%;">
                    <canvas id="sct-sb-canvas" style="position:absolute; top:0; left:0; width:100%; height:100%;"></canvas>
                </div>
            </div>
        </div>

        <div id="jump-section" style="display:none; margin-top:14px;">
            <h2>Jump Bid</h2>
            <div class="controls-row">
                <div class="input-group">
                    <label>Team</label>
                    <select id="jump-team" style="width:200px;"></select>
                </div>
                <button class="btn btn-primary" onclick="jumpBid()"
                    style="background:#66bb6a; margin-top:18px;">Jump Bid</button>
            </div>
        </div>

        <div id="manual-section" style="display:none; margin-top:14px;">
            <h2>Manual Trade</h2>
            <div class="controls-row">
                <div class="input-group">
                    <label>Team</label>
                    <select id="manual-team" style="width:200px;"></select>
                </div>
                <div class="input-group">
                    <label>Price (c)</label>
                    <input type="number" id="manual-price" placeholder="&mdash;" min="1" max="99">
                </div>
                <button class="btn btn-primary" onclick="manualBuy()"
                    style="margin-top:18px;">Buy Limit</button>
                <button class="btn btn-ioc" onclick="manualIOC()"
                    style="margin-top:18px;">Buy IOC</button>
            </div>
        </div>

        <div id="loading"><div class="spinner"></div> Processing...</div>
        <div id="results"></div>

        <div id="orderbook-section" style="display:none;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-top:12px;">
                <h2>Live Orderbook</h2>
                <span style="font-size:11px; color:#666;">Auto-refreshes 3s &bull;
                    <span id="ob-last-update"></span></span>
            </div>
            <div id="orderbook-content"></div>
        </div>
    </div>
    </div>

    <div id="tab-ou-trade" style="display:none;">
    <div id="ou-match-content" style="display:none;">
        <div class="results" id="ou-fair-section">
            <div class="fair-price">
                <span class="home" id="ou-over-fair">&mdash;</span>
                <span class="sep">/</span>
                <span class="away" id="ou-under-fair">&mdash;</span>
            </div>
            <div class="market-info">
                <span>Pregame O/U: <span id="ou-pregame-pct">&mdash;</span></span>
                <span>Live O/U: <span id="ou-live-pct">&mdash;</span></span>
                <span>Score: <span id="ou-score-label">0-0</span></span>
            </div>
        </div>

        <div class="controls-row">
            <div class="input-group">
                <label>Contracts</label>
                <input type="number" id="ou-contracts" value="{{ contracts }}" min="1" max="500">
            </div>
            <div class="input-group">
                <label>Spread (c)</label>
                <input type="number" id="ou-spread-cents" value="{{ spread }}" min="1" max="50">
            </div>
        </div>

        <div class="btn-row">
            <button class="btn btn-compute" onclick="ouComputeFair()">Compute Fair</button>
            <button class="btn btn-primary" onclick="ouStartAutoReprice()">Post Liquidity</button>
            <button class="btn btn-ioc" onclick="ouPostIOC()">IOC Take</button>
            <button class="btn btn-cancel-post" onclick="ouCancelRepost()">Cancel &amp; Repost</button>
            <button class="btn btn-danger" onclick="ouCancelAll()">Cancel All</button>
        </div>

        <div class="kbd-help">
            <kbd>C</kbd> Compute &nbsp; <kbd>P</kbd> Auto-Reprice &nbsp;
            <kbd>I</kbd> IOC &nbsp; <kbd>R</kbd> Repost &nbsp; <kbd>X</kbd> Stop+Cancel &nbsp;
            <kbd>J</kbd> Jump &nbsp; <kbd>B</kbd> Buy
            &nbsp; <span id="ou-reprice-status"></span>
        </div>

        <div id="ou-jump-section" style="display:none; margin-top:14px;">
            <h2>Jump Bid</h2>
            <div class="controls-row">
                <div class="input-group">
                    <label>Side</label>
                    <select id="ou-jump-team" style="width:200px;"></select>
                </div>
                <button class="btn btn-primary" onclick="ouJumpBid()"
                    style="background:#66bb6a; margin-top:18px;">Jump Bid</button>
            </div>
        </div>

        <div id="ou-manual-section" style="display:none; margin-top:14px;">
            <h2>Manual Trade</h2>
            <div class="controls-row">
                <div class="input-group">
                    <label>Side</label>
                    <select id="ou-manual-team" style="width:200px;"></select>
                </div>
                <div class="input-group">
                    <label>Price (c)</label>
                    <input type="number" id="ou-manual-price" placeholder="&mdash;" min="1" max="99">
                </div>
                <button class="btn btn-primary" onclick="ouManualBuy()"
                    style="margin-top:18px;">Buy Limit</button>
                <button class="btn btn-ioc" onclick="ouManualIOC()"
                    style="margin-top:18px;">Buy IOC</button>
            </div>
        </div>

        <div id="ou-loading" style="display:none;"><div class="spinner"></div> Processing...</div>
        <div id="ou-results"></div>

        <div id="ou-orderbook-section" style="display:none;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-top:12px;">
                <h2>Live Orderbook</h2>
                <span style="font-size:11px; color:#666;">Auto-refreshes 3s &bull;
                    <span id="ou-ob-last-update"></span></span>
            </div>
            <div id="ou-orderbook-content"></div>
        </div>
    </div>
    </div>

    <div id="tab-forfeit" style="display:none;">
        <h2>Forfeit Iceberg</h2>
        <p style="color:#aaa; font-size:13px; margin-bottom:14px;">
            Accumulate a position with small randomized IOC orders at a set interval.
        </p>
        <div id="ff-match-content" style="display:none;">
            <div class="controls-row">
                <div class="input-group">
                    <label>Market</label>
                    <select id="ff-market-type" onchange="ffPopulateTeams()" style="width:160px; padding:6px 8px; background:#1a1a2e; color:#e0e0e0; border:1px solid #333; border-radius:4px;">
                        <option value="moneyline">Moneyline</option>
                        <option value="totalmaps">Total Maps</option>
                    </select>
                </div>
                <div class="input-group">
                    <label>Side</label>
                    <select id="ff-team" style="width:220px; padding:6px 8px; background:#1a1a2e; color:#e0e0e0; border:1px solid #333; border-radius:4px;"></select>
                </div>
                <div class="input-group">
                    <label>Max Price (c)</label>
                    <input type="number" id="ff-price" value="97" min="1" max="99">
                </div>
                <div class="input-group">
                    <label>Delay (s)</label>
                    <input type="number" id="ff-delay" value="30" min="1" max="300">
                </div>
            </div>

            <div class="btn-row">
                <button class="btn btn-primary" onclick="ffStart()" id="ff-start-btn">Start Iceberg</button>
                <button class="btn btn-danger" onclick="ffStop()" id="ff-stop-btn" style="display:none;">Stop</button>
            </div>

            <div class="kbd-help">
                <span id="ff-status"></span>
            </div>

            <div id="ff-stats" class="results" style="margin-top:12px; display:none;">
                <div class="market-info">
                    <span>Total Filled: <b><span id="ff-filled-count">0</span></b></span>
                    <span>Avg Price: <b><span id="ff-avg-price">&mdash;</span></b></span>
                    <span>Total Cost: <b><span id="ff-total-cost">$0.00</span></b></span>
                    <span>Orders Sent: <b><span id="ff-orders-sent">0</span></b></span>
                </div>
            </div>

            <div id="ff-log" style="margin-top:12px; max-height:300px; overflow-y:auto; font-family:'SF Mono','Menlo',monospace; font-size:12px; color:#aaa;"></div>

            <div id="ff-orderbook-section" style="display:none; margin-top:12px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <h2>Live Orderbook</h2>
                    <span style="font-size:11px; color:#666;">Auto-refreshes 3s &bull;
                        <span id="ff-ob-last-update"></span></span>
                </div>
                <div id="ff-orderbook-content"></div>
            </div>
        </div>
    </div>

    <div id="tab-screen" style="display:none;">
        <h2 style="color:#4fc3f7; margin-bottom:4px;">Screen Odds Tracker</h2>
        <p style="font-size:12px; color:#888; margin-bottom:14px;">
            Capture decimal odds from a betting feed. Mark the scoreboard region, tag home/away odds, then track live.
        </p>

        <div style="display:flex; gap:6px; align-items:center; margin-bottom:10px;">
            <span id="st-stream-fps" style="font-size:11px; color:#444; margin-right:6px;"></span>
            <button class="btn" id="st-stream-toggle" onclick="stToggleStream()" style="padding:5px 14px;font-size:12px;background:#333;">
                Start Live View</button>
            <span id="st-tracker-status" style="font-size:12px; color:#666;">Stopped</span>
            <button class="btn btn-primary" id="st-tracker-toggle" onclick="stToggleTracker()" style="padding:5px 14px;font-size:12px;">Start Tracking</button>
            <button class="btn" onclick="stReset()" style="padding:5px 10px;font-size:11px;background:#333;color:#ffab40;">Reset</button>
        </div>

        <p style="font-size:11px; color:#666; margin:6px 0 8px;">
            <b>Step 1:</b> Click "Mark Scoreboard" then draw a box around the odds area. Scroll to zoom, drag to pan.
        </p>
        <div style="display:flex; gap:8px; margin-bottom:8px; align-items:center;">
            <button class="btn st-region-btn" id="st-rgn-btn-scoreboard" onclick="stSetRegionMode('scoreboard')"
                    style="padding:4px 12px;font-size:11px;background:#333;border:1px solid #555;">Mark Scoreboard</button>
            <button class="btn" onclick="stClearRegions()" style="padding:4px 10px;font-size:11px;background:#333;color:#ef5350;">Clear All</button>
            <span id="st-zoom-level" style="font-size:11px; color:#666; margin-left:auto;"></span>
            <button class="btn" onclick="stZoomIn()" style="padding:2px 8px;font-size:13px;background:#333;">+</button>
            <button class="btn" onclick="stZoomOut()" style="padding:2px 8px;font-size:13px;background:#333;">−</button>
            <button class="btn" onclick="stZoomReset()" style="padding:2px 8px;font-size:11px;background:#333;">1:1</button>
        </div>

        <div id="st-screenshot-container" style="position:relative; max-width:100%; overflow:hidden;
             border:1px solid #333; border-radius:4px; height:350px; background:#111;">
            <span id="st-stream-placeholder" style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
                  color:#555; font-size:13px; z-index:1; pointer-events:none;">Click "Start Live View" or "Mark Scoreboard" to begin</span>
            <div id="st-screen-inner" style="transform-origin:0 0; position:relative; width:100%;">
                <img id="st-screen-img" style="display:none; width:100%;">
                <canvas id="st-screen-canvas" style="position:absolute; top:0; left:0; width:100%; height:100%;"></canvas>
            </div>
        </div>

        <div id="st-sb-section" style="display:none; margin-top:12px;">
            <p style="font-size:11px; color:#666; margin:0 0 8px;">
                <b>Step 2:</b> Draw boxes on the scoreboard preview to tag each odds region.
            </p>
            <div style="display:flex; gap:8px; margin-bottom:8px; align-items:center; flex-wrap:wrap;">
                <button class="btn st-sub-btn" id="st-sub-btn-home_odds" onclick="stSetSubMode('home_odds')"
                        style="padding:4px 10px;font-size:11px;background:#333;border:1px solid #555;">Home Odds</button>
                <button class="btn st-sub-btn" id="st-sub-btn-away_odds" onclick="stSetSubMode('away_odds')"
                        style="padding:4px 10px;font-size:11px;background:#333;border:1px solid #555;">Away Odds</button>
                <button class="btn" onclick="stTestOCR()" style="padding:4px 10px;font-size:11px;background:#555;">Test OCR</button>
            </div>
            <div id="st-region-previews" style="display:flex; gap:10px; margin-bottom:8px; flex-wrap:wrap;"></div>
            <div id="st-sb-preview" style="position:relative; border:1px solid #4fc3f7; border-radius:4px;
                 background:#111; overflow:hidden; cursor:crosshair;">
                <img id="st-sb-img" style="display:block; width:100%;">
                <canvas id="st-sb-canvas" style="position:absolute; top:0; left:0; width:100%; height:100%;"></canvas>
            </div>
        </div>

        <div id="st-tracker-live" style="display:none; margin-top:10px; padding:10px; background:#111; border-radius:4px;">
            <div style="display:flex; gap:20px; align-items:center; font-size:14px;">
                <span>Home: <b id="st-ocr-home" style="color:#4caf50;">—</b></span>
                <span>Away: <b id="st-ocr-away" style="color:#ef5350;">—</b></span>
                <span id="st-ocr-conf" style="font-size:11px; color:#666;"></span>
            </div>
        </div>

        <div id="st-history-section" style="margin-top:16px;">
            <h3 style="color:#aaa; font-size:13px; margin-bottom:8px;">Odds History</h3>
            <div id="st-history-log" style="max-height:300px; overflow-y:auto; font-family:'SF Mono','Menlo',monospace;
                 font-size:12px; color:#aaa; background:#0a0a0a; border-radius:4px; padding:8px;"></div>
        </div>
    </div>

<script>
let currentGame = null;
let mapResults = [0, 0, 0, 0, 0];
let homeRounds = 0, awayRounds = 0;
let homeAlive = 5, awayAlive = 5;
let homeFair = 50;
let obInterval = null;
let autoRepricing = false;
let lastPostedFair = null;
let repriceInFlight = false;
let activeTab = 'trade';

// O/U state
let ouOverFair = 50;
let ouObInterval = null;
let ouAutoRepricing = false;
let ouLastPostedFair = null;
let ouRepriceInFlight = false;

// Forfeit iceberg state
let ffActive = false;
let ffTimeout = null;
let ffObInterval = null;
let ffTotalFilled = 0;
let ffTotalCost = 0;
let ffOrdersSent = 0;

function onGameSelect() {
    const sel = document.getElementById('game-select');
    const opt = sel.options[sel.selectedIndex];
    if (!opt.value) {
        currentGame = null;
        document.getElementById('match-section').style.display = 'none';
        document.getElementById('trade-match-content').style.display = 'none';
        document.getElementById('ou-match-content').style.display = 'none';
        stopOB();
        ouStopOB();
        return;
    }
    const [home, away] = opt.value.split('|');
    currentGame = {
        home, away,
        homeProb: parseFloat(opt.dataset.homeProb),
        tickers: JSON.parse(opt.dataset.tickers || '[]'),
        ouTickers: JSON.parse(opt.dataset.ouTickers || '[]'),
        polySeriesTickers: JSON.parse(opt.dataset.polySeries || '[]'),
    };
    mapResults = [0, 0, 0, 0, 0];
    homeRounds = 0;
    awayRounds = 0;
    document.getElementById('home-rounds').textContent = '0';
    document.getElementById('away-rounds').textContent = '0';
    resetAlive();
    onBestOfChange();

    document.getElementById('home-label').textContent = home;
    document.getElementById('away-label').textContent = away;
    document.getElementById('round-home-label').textContent = home;
    document.getElementById('round-away-label').textContent = away;
    document.getElementById('alive-home-label').textContent = home;
    document.getElementById('alive-away-label').textContent = away;
    const ctSel = document.getElementById('ct-team-select');
    ctSel.innerHTML = '<option value="home">'+home+'</option><option value="away">'+away+'</option>';
    document.getElementById('map-select').value = '';
    document.getElementById('ct-win-pct').value = '';
    document.getElementById('t-win-pct').value = '';
    document.getElementById('half-label').textContent = '';
    document.getElementById('home-buy-label').textContent = home + ':';
    document.getElementById('away-buy-label').textContent = away + ':';
    document.getElementById('home-buy').value = '';
    document.getElementById('away-buy').value = '';
    document.getElementById('match-section').style.display = 'block';
    document.getElementById('pregame-chip').style.display = 'inline-block';
    document.getElementById('pg-prob').textContent =
        (currentGame.homeProb * 100).toFixed(0) + '%';

    populateTeamDropdowns();
    populateOUDropdowns();
    ffPopulateTeams();

    const hasTickers = currentGame.tickers.length > 0;
    const hasPoly = currentGame.polySeriesTickers.length > 0;
    const hasAny = hasTickers || hasPoly;
    document.getElementById('orderbook-section').style.display = hasAny ? 'block' : 'none';
    document.getElementById('manual-section').style.display = hasTickers ? 'block' : 'none';
    document.getElementById('jump-section').style.display = hasTickers ? 'block' : 'none';
    document.getElementById('trade-match-content').style.display = 'block';
    if (hasAny) startOB(); else stopOB();

    const hasOUTickers = currentGame.ouTickers.length > 0;
    document.getElementById('ou-orderbook-section').style.display = hasOUTickers ? 'block' : 'none';
    document.getElementById('ou-manual-section').style.display = hasOUTickers ? 'block' : 'none';
    document.getElementById('ou-jump-section').style.display = hasOUTickers ? 'block' : 'none';
    document.getElementById('ou-match-content').style.display = 'block';
    if (hasOUTickers) ouStartOB(); else ouStopOB();

    const hasAnyFF = hasTickers || hasOUTickers;
    document.getElementById('ff-match-content').style.display = hasAnyFF ? 'block' : 'none';
    document.getElementById('ff-orderbook-section').style.display = hasAnyFF ? 'block' : 'none';
    if (hasAnyFF && activeTab === 'forfeit') ffStartOB();

    updateDisplay();
    computeFair();
    ouComputeFair();
}

function populateTeamDropdowns() {
    if (!currentGame || !currentGame.tickers.length) return;
    const teams = [];
    for (const t of currentGame.tickers) {
        teams.push({label: t.yes_team, side: 'yes', ticker: t.ticker});
        teams.push({label: t.no_team, side: 'no', ticker: t.ticker});
    }
    const seen = new Set();
    const unique = [];
    for (const t of teams) {
        if (!seen.has(t.label)) { seen.add(t.label); unique.push(t); }
    }
    for (const selId of ['manual-team', 'jump-team']) {
        const el = document.getElementById(selId);
        el.innerHTML = '';
        for (const t of unique) {
            const o = document.createElement('option');
            o.value = JSON.stringify({side: t.side, ticker: t.ticker});
            o.textContent = t.label;
            el.appendChild(o);
        }
    }
}

function populateOUDropdowns() {
    if (!currentGame || !currentGame.ouTickers.length) return;
    const sides = [];
    for (const t of currentGame.ouTickers) {
        sides.push({label: 'Over 2.5', side: 'yes', ticker: t.ticker});
        sides.push({label: 'Under 2.5', side: 'no', ticker: t.ticker});
    }
    for (const selId of ['ou-manual-team', 'ou-jump-team']) {
        const el = document.getElementById(selId);
        el.innerHTML = '';
        for (const s of sides) {
            const o = document.createElement('option');
            o.value = JSON.stringify({side: s.side, ticker: s.ticker});
            o.textContent = s.label;
            el.appendChild(o);
        }
    }
}

function onBestOfChange() {
    const bo = parseInt(document.getElementById('best-of-select').value) || 3;
    const maxMaps = bo;
    for (let i = 0; i < 5; i++) {
        const slot = document.querySelector('[data-map="'+(i+1)+'"]');
        if (slot) slot.style.display = i < maxMaps ? '' : 'none';
    }
    mapResults = [0, 0, 0, 0, 0];
    homeRounds = 0;
    awayRounds = 0;
    document.getElementById('home-rounds').textContent = '0';
    document.getElementById('away-rounds').textContent = '0';
    updateDisplay();
    computeFair();
    ouComputeFair();
}

function cycleMap(n) {
    const idx = n - 1;
    mapResults[idx] = mapResults[idx] === 0 ? 1 : mapResults[idx] === 1 ? -1 : 0;
    homeRounds = 0;
    awayRounds = 0;
    document.getElementById('home-rounds').textContent = '0';
    document.getElementById('away-rounds').textContent = '0';
    updateDisplay();
    computeFair();
    ouComputeFair();
}

function adjRounds(who, delta) {
    if (who === 'home') homeRounds = Math.max(0, Math.min(30, homeRounds + delta));
    else awayRounds = Math.max(0, Math.min(30, awayRounds + delta));
    document.getElementById('home-rounds').textContent = homeRounds;
    document.getElementById('away-rounds').textContent = awayRounds;
    if (delta > 0) resetAlive();
    computeFair();
    ouComputeFair();
}

const MAP_CT_RATES = {mirage:55, nuke:57, inferno:53, ancient:54, anubis:52, dust2:51, vertigo:54, train:53};
function onMapSelect() {
    const map = document.getElementById('map-select').value;
    if (map && MAP_CT_RATES[map]) {
        document.getElementById('ct-win-pct').value = MAP_CT_RATES[map];
        document.getElementById('t-win-pct').value = 100 - MAP_CT_RATES[map];
    }
    computeFair();
    ouComputeFair();
}

function adjAlive(who, delta) {
    if (who === 'home') homeAlive = Math.max(0, Math.min(5, homeAlive + delta));
    else awayAlive = Math.max(0, Math.min(5, awayAlive + delta));
    document.getElementById('home-alive').textContent = homeAlive;
    document.getElementById('away-alive').textContent = awayAlive;
    computeFair();
    ouComputeFair();
}

function resetAlive() {
    homeAlive = 5; awayAlive = 5;
    document.getElementById('home-alive').textContent = '5';
    document.getElementById('away-alive').textContent = '5';
}

function onMoneyInput(who) {
    const money = parseInt(document.getElementById(who + '-money').value);
    if (!money && money !== 0) return;
    const avg = money / 5;
    const ctTeam = document.getElementById('ct-team-select').value;
    const side = (who === ctTeam) ? 'ct' : 't';
    const threshold = side === 't' ? 3700 : 3900;
    let buy = 'eco';
    if (avg >= threshold) buy = 'full';
    else if (avg >= 2400) buy = 'force';
    document.getElementById(who + '-buy').value = buy;
    computeFair();
    ouComputeFair();
}

function onBuyChange(who) {
    document.getElementById(who + '-money').value = '';
    computeFair();
    ouComputeFair();
}

function updateDisplay() {
    if (!currentGame) return;
    const bo = parseInt(document.getElementById('best-of-select').value) || 3;
    let homeMaps = 0, awayMaps = 0;
    for (let i = 0; i < bo; i++) {
        const slot = document.querySelector('[data-map="'+(i+1)+'"]');
        const result = document.getElementById('map'+(i+1)+'-result');
        if (!slot) continue;
        slot.classList.remove('home-won', 'away-won');
        if (mapResults[i] === 1) {
            slot.classList.add('home-won');
            result.textContent = currentGame.home;
            homeMaps++;
        } else if (mapResults[i] === -1) {
            slot.classList.add('away-won');
            result.textContent = currentGame.away;
            awayMaps++;
        } else {
            result.textContent = '\\u2014';
        }
    }
    document.getElementById('home-score').textContent = homeMaps;
    document.getElementById('away-score').textContent = awayMaps;
    const hs = document.getElementById('home-score');
    const as_ = document.getElementById('away-score');
    hs.classList.toggle('leading', homeMaps > awayMaps);
    as_.classList.toggle('leading', awayMaps > homeMaps);
}

function getState() {
    const bo = parseInt(document.getElementById('best-of-select').value) || 3;
    let homeMaps = 0, awayMaps = 0, mapsPlayed = 0;
    for (let i = 0; i < bo; i++) {
        if (mapResults[i] !== 0) {
            mapsPlayed = i + 1;
            if (mapResults[i] === 1) homeMaps++;
            else awayMaps++;
        } else break;
    }
    const ov = document.getElementById('override-prob').value;
    const homeProb = ov ? parseInt(ov) / 100 : currentGame.homeProb;
    const bestOf = parseInt(document.getElementById('best-of-select').value) || 3;
    const ctTeam = document.getElementById('ct-team-select').value;
    const ctPct = document.getElementById('ct-win-pct').value;
    const tPct = document.getElementById('t-win-pct').value;
    const totalRounds = homeRounds + awayRounds;
    const secondHalf = totalRounds >= 12;
    const homeIsCt = secondHalf ? (ctTeam !== 'home') : (ctTeam === 'home');
    document.getElementById('half-label').textContent =
        totalRounds >= 12 ? '2nd half (sides flipped)' : totalRounds > 0 ? '1st half' : '';
    return {
        home: currentGame.home,
        away: currentGame.away,
        home_prob: homeProb,
        home_maps: homeMaps,
        away_maps: awayMaps,
        maps_played: mapsPlayed,
        home_rounds: homeRounds,
        away_rounds: awayRounds,
        contracts: parseInt(document.getElementById('contracts').value),
        spread_cents: parseInt(document.getElementById('spread-cents').value),
        tickers: currentGame.tickers,
        poly_tickers: currentGame.polySeriesTickers,
        best_of: bestOf,
        home_is_ct: homeIsCt,
        ct_win_pct: ctPct ? parseInt(ctPct) : null,
        t_win_pct: tPct ? parseInt(tPct) : null,
        home_buy: document.getElementById('home-buy').value || null,
        away_buy: document.getElementById('away-buy').value || null,
        home_money: parseInt(document.getElementById('home-money').value) || null,
        away_money: parseInt(document.getElementById('away-money').value) || null,
        home_alive: homeAlive,
        away_alive: awayAlive,
    };
}

async function computeFair() {
    if (!currentGame) return;
    try {
        const resp = await fetch('/api/compute_fair', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        const data = await resp.json();
        homeFair = data.home_fair;
        document.getElementById('home-fair').textContent =
            currentGame.home + ' ' + data.home_fair + 'c';
        document.getElementById('away-fair').textContent =
            currentGame.away + ' ' + data.away_fair + 'c';
        document.getElementById('pregame-pct').textContent =
            (data.pregame_prob * 100).toFixed(0) + '%';
        document.getElementById('map-pct').textContent =
            (data.map_prob * 100).toFixed(0) + '%';
        document.getElementById('live-pct').textContent =
            (data.live_prob * 100).toFixed(0) + '%';
        document.getElementById('score-label').textContent =
            data.home_maps + '-' + data.away_maps +
            (homeRounds || awayRounds ? ' (R' + homeRounds + '-' + awayRounds + ')' : '');

        if (data.alt_lines && data.alt_lines.scorelines) {
            let html = '';
            for (const s of data.alt_lines.scorelines) {
                const parts = s.label.split('-');
                const winner = parseInt(parts[0]) > parseInt(parts[1])
                    ? currentGame.home : currentGame.away;
                html += '<div class="scoreline-chip">' +
                    '<div class="sl-winner">' + winner + '</div>' +
                    '<div class="sl-score">' + s.label + '</div>' +
                    '<div class="sl-pct">' + (s.prob * 100).toFixed(1) + '%</div>' +
                    '<div class="sl-fair">' + s.fair + 'c</div></div>';
            }
            document.getElementById('scoreline-grid').innerHTML = html;
        }

        if (data.alt_lines && data.alt_lines.over_under) {
            const ou = data.alt_lines.over_under;
            document.getElementById('ou-row').innerHTML =
                '<div class="ou-chip"><div class="ou-label">Over 2.5</div>' +
                '<div class="ou-pct">' + (ou.over_prob * 100).toFixed(1) + '%</div>' +
                '<div class="ou-fair">' + ou.over_fair + 'c</div></div>' +
                '<div class="ou-chip"><div class="ou-label">Under 2.5</div>' +
                '<div class="ou-pct">' + (ou.under_prob * 100).toFixed(1) + '%</div>' +
                '<div class="ou-fair">' + ou.under_fair + 'c</div></div>';
        }

        const econDiv = document.getElementById('econ-info');
        if (data.econ && data.econ.home_buy) {
            const e = data.econ;
            const buyColors = {full: '#81c784', force: '#ff9800', eco: '#ef5350'};
            const aliveStr = (n) => e.phase === 'live' ? ' (' + n + ' alive)' : '';
            document.getElementById('home-econ-label').innerHTML =
                currentGame.home + ': <span style="color:' + (buyColors[e.home_buy]||'#aaa') +
                '">' + e.home_buy.toUpperCase() + '</span> $' + e.home_avg_money +
                aliveStr(e.home_alive || 5);
            document.getElementById('away-econ-label').innerHTML =
                currentGame.away + ': <span style="color:' + (buyColors[e.away_buy]||'#aaa') +
                '">' + e.away_buy.toUpperCase() + '</span> $' + e.away_avg_money +
                aliveStr(e.away_alive || 5);
            let bombTag = '';
            if (e.bomb_status === 'planted') bombTag = ' 💣 PLANTED';
            else if (e.bomb_status === 'time_low') bombTag = ' ⏱ LOW TIME';
            else if (e.bomb_status === 'time_mid') bombTag = ' ⏱ time pressure';
            const timerStr = e.timer_seconds >= 0 ? ' (' + e.timer_seconds + 's)' : '';
            document.getElementById('econ-adj-pct').textContent =
                (e.home_round_p * 100).toFixed(1) + '% round' +
                (e.phase === 'round_over' ? ' (next)' : e.phase === 'buy_phase' ? ' (buying)' : '') + bombTag + timerStr;
            econDiv.style.display = 'flex';

        } else {
            econDiv.style.display = 'none';
        }
        // Auto-reprice: if active and fair changed, cancel and repost
        if (autoRepricing && !repriceInFlight && lastPostedFair !== null) {
            if (homeFair !== lastPostedFair) {
                console.log('[REPRICE] Fair moved ' + lastPostedFair + 'c -> ' + homeFair + 'c, repricing...');
                repriceInFlight = true;
                doReprice().finally(() => { repriceInFlight = false; });
            }
        }
    } catch(e) {
        console.error('computeFair error:', e);
    }
}

async function doReprice() {
    try {
        const resp = await fetch('/api/cancel_and_repost', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        const data = await resp.json();
        if (data.home_fair !== undefined) {
            lastPostedFair = data.home_fair;
            homeFair = data.home_fair;
        }
        renderResults(data);
    } catch(e) {
        console.error('doReprice error:', e);
    }
}

function updateRepriceIndicator() {
    const el = document.getElementById('reprice-status');
    if (el) {
        if (autoRepricing) {
            el.textContent = 'AUTO-REPRICE ON';
            el.style.display = 'inline-block';
            el.style.background = '#4caf50';
            el.style.color = '#fff';
            el.style.padding = '2px 8px';
            el.style.borderRadius = '4px';
            el.style.fontSize = '11px';
            el.style.fontWeight = '700';
        } else {
            el.textContent = '';
            el.style.display = 'none';
        }
    }
}

function showLoading() { document.getElementById('loading').style.display = 'block'; }
function hideLoading() { document.getElementById('loading').style.display = 'none'; }

function renderResults(data) {
    if (data.error) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">' + data.error + '</div>';
        return;
    }
    let html = '';
    if (data.cancelled)
        html += '<div class="alert alert-info">Cancelled existing orders first</div>';
    if (data.message)
        html += '<div class="alert alert-success">' + data.message + '</div>';

    if (data.home_fair !== undefined) {
        document.getElementById('home-fair').textContent =
            data.home + ' ' + data.home_fair + 'c';
        document.getElementById('away-fair').textContent =
            data.away + ' ' + data.away_fair + 'c';
        homeFair = data.home_fair;
    }

    if (data.orders && data.orders.length > 0) {
        html += '<table class="orders-table"><thead><tr>' +
            '<th>Selection</th><th>Price</th><th>Qty</th><th>Status</th>' +
            '</tr></thead><tbody>';
        for (const o of data.orders) {
            const cls = (o.status === 'placed' || o.status === 'filled')
                ? 'placed' : 'failed';
            let st = o.status.toUpperCase();
            if (o.filled !== undefined) st = o.filled + '/' + o.contracts + ' FILLED';
            html += '<tr><td>' + o.team + '</td><td>' + o.price + 'c</td><td>' +
                o.contracts + '</td><td class="status-' + cls + '">' + st + '</td></tr>';
        }
        html += '</tbody></table>';
    }
    document.getElementById('results').innerHTML = html;
}

async function startAutoReprice() {
    if (!currentGame) return;
    if (autoRepricing) {
        // Already active — just do a manual repost
        await postOrders();
        return;
    }
    autoRepricing = true;
    updateRepriceIndicator();
    console.log('[REPRICE] Auto-reprice STARTED');
    await postOrders();
}

function stopAutoReprice() {
    const wasActive = autoRepricing;
    autoRepricing = false;
    lastPostedFair = null;
    repriceInFlight = false;
    updateRepriceIndicator();
    if (wasActive) console.log('[REPRICE] Auto-reprice STOPPED');
    cancelAll();
}

async function postOrders() {
    if (!currentGame) return;
    showLoading();
    try {
        const resp = await fetch('/api/compute_and_post', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        const data = await resp.json();
        renderResults(data);
        if (autoRepricing && data.home_fair !== undefined) {
            lastPostedFair = data.home_fair;
        }
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
}

async function postIOC() {
    if (!currentGame) return;
    showLoading();
    try {
        const resp = await fetch('/api/compute_and_ioc', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        renderResults(await resp.json());
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
}

async function cancelRepost() {
    if (!currentGame) return;
    showLoading();
    try {
        const resp = await fetch('/api/cancel_and_repost', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        renderResults(await resp.json());
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
}

async function cancelAll() {
    showLoading();
    try {
        const promises = [];
        const tickers = currentGame ? currentGame.tickers : [];
        promises.push(fetch('/api/cancel_all', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers}),
        }).then(r => r.json()));
        if (currentGame && currentGame.polySeriesTickers && currentGame.polySeriesTickers.length) {
            const tokenIds = [];
            for (const t of currentGame.polySeriesTickers) { tokenIds.push(t.token_a); tokenIds.push(t.token_b); }
            promises.push(fetch('/api/poly_cancel_all', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({token_ids: tokenIds}),
            }).then(r => r.json()));
        }
        const results = await Promise.all(promises);
        const msgs = results.map(r => r.message).filter(Boolean).join(' | ');
        document.getElementById('results').innerHTML =
            '<div class="alert alert-success">' + (msgs || 'Cancelled') + '</div>';
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
}

// ── Auto-Trade ──
let autoTrading = false;
let autoTradeInterval = null;
let autoTradeBusy = false;

function toggleAutoTrade() {
    autoTrading ? stopAutoTrade() : startAutoTrade();
}

function startAutoTrade() {
    if (!currentGame) { alert('Select a match first'); return; }
    autoTrading = true;
    autoTradeBusy = false;
    const btn = document.getElementById('auto-trade-btn');
    btn.textContent = 'STOP';
    btn.style.background = '#b71c1c';
    btn.style.color = '#ff5252';
    btn.style.borderColor = '#ff5252';
    autoTradeExecute();
    autoTradeInterval = setInterval(autoTradeExecute, 3000);
}

async function stopAutoTrade() {
    autoTrading = false;
    autoTradeBusy = false;
    if (autoTradeInterval) { clearInterval(autoTradeInterval); autoTradeInterval = null; }
    const btn = document.getElementById('auto-trade-btn');
    btn.textContent = 'AUTO TRADE';
    btn.style.background = '#1b5e20';
    btn.style.color = '#66bb6a';
    btn.style.borderColor = '#66bb6a';
    try {
        const promises = [];
        const tickers = currentGame ? currentGame.tickers : [];
        promises.push(fetch('/api/cancel_all', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers}),
        }));
        if (currentGame && currentGame.polySeriesTickers && currentGame.polySeriesTickers.length) {
            const tokenIds = [];
            for (const t of currentGame.polySeriesTickers) { tokenIds.push(t.token_a); tokenIds.push(t.token_b); }
            promises.push(fetch('/api/poly_cancel_all', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({token_ids: tokenIds}),
            }));
        }
        await Promise.all(promises);
        document.getElementById('results').innerHTML =
            '<div class="alert alert-success">Auto-trade stopped. All orders cancelled.</div>';
    } catch(e) {}
}

async function autoTradeExecute() {
    if (!autoTrading || !currentGame || autoTradeBusy) return;
    autoTradeBusy = true;
    try {
        const resp = await fetch('/api/auto_trade', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        const data = await resp.json();
        renderResults(data);
        if (data.home_fair !== undefined) {
            homeFair = data.home_fair;
        }
    } catch(e) {}
    autoTradeBusy = false;
}

async function jumpBid() {
    if (!currentGame || !currentGame.tickers.length) return;
    const sel = JSON.parse(document.getElementById('jump-team').value);
    const contracts = parseInt(document.getElementById('contracts').value);
    showLoading();
    try {
        const resp = await fetch('/api/jump_bid', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ticker: sel.ticker, side: sel.side, contracts}),
        });
        renderResults(await resp.json());
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
}

async function manualBuy() { await manualOrder(false); }
async function manualIOC() { await manualOrder(true); }

async function manualOrder(ioc) {
    if (!currentGame || !currentGame.tickers.length) return;
    const sel = JSON.parse(document.getElementById('manual-team').value);
    const price = parseInt(document.getElementById('manual-price').value);
    const contracts = parseInt(document.getElementById('contracts').value);
    if (!price || price < 1 || price > 99) { alert('Enter price 1-99c'); return; }
    showLoading();
    try {
        const resp = await fetch('/api/manual_order', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                ticker: sel.ticker, side: sel.side, price, contracts, ioc}),
        });
        renderResults(await resp.json());
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
}

// ── Orderbook ──────────────────────────────────────────────
function startOB() { stopOB(); fetchOB(); obInterval = setInterval(fetchOB, 3000); }
function stopOB() { if (obInterval) { clearInterval(obInterval); obInterval = null; } }

async function fetchOB() {
    if (!currentGame) return;
    const hasKalshi = currentGame.tickers.length > 0;
    const hasPoly = currentGame.polySeriesTickers && currentGame.polySeriesTickers.length > 0;
    if (!hasKalshi && !hasPoly) return;
    try {
        const promises = [];
        if (hasKalshi) {
            promises.push(fetch('/api/orderbook', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({tickers: currentGame.tickers}),
            }).then(r => r.json()));
        }
        if (hasPoly) {
            promises.push(fetch('/api/poly_orderbook', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({poly_tickers: currentGame.polySeriesTickers}),
            }).then(r => r.json()));
        }
        const results = await Promise.all(promises);
        let allBooks = [];
        for (const r of results) { if (r.books) allBooks = allBooks.concat(r.books); }
        renderOB(allBooks);
        document.getElementById('ob-last-update').textContent =
            new Date().toLocaleTimeString();
    } catch(e) {}
}

function renderOB(books, containerId) {
    const container = document.getElementById(containerId || 'orderbook-content');
    let html = '';
    for (const book of books) {
        html += '<div class="ob-panel">';
        html += '<h3>' + book.yes_team + ' (YES) vs ' + book.no_team + ' (NO)</h3>';
        if (book.error) {
            html += '<div style="color:#ef5350;text-align:center;padding:10px;">' +
                book.error + '</div></div>';
            continue;
        }
        const bids = book.yes_bids || [];
        const asks = book.yes_asks || [];
        const myBid = new Set(book.my_bid_prices || []);
        const myAsk = new Set(book.my_ask_prices || []);
        const allQty = bids.concat(asks).map(x => x[1]);
        const maxQty = Math.max(...allQty, 1);
        const bestBid = bids.length > 0 ? bids[0][0] : 0;
        const bestAsk = asks.length > 0 ? asks[0][0] : 100;

        html += '<div class="ob-book"><div class="ob-side">';
        html += '<div class="ob-side-header bids"><span>Bid</span><span>Qty</span></div>';
        for (const [price, qty] of bids.slice(0, 8)) {
            const pct = (qty / maxQty * 100).toFixed(0);
            const mine = myBid.has(price) ? ' mine' : '';
            html += '<div class="ob-row bid-row' + mine +
                '"><div class="depth-bar" style="width:' + pct +
                '%"></div><span class="price">' + price +
                'c</span><span class="qty">' + qty + '</span></div>';
        }
        html += '</div><div class="ob-side">';
        html += '<div class="ob-side-header asks"><span>Ask</span><span>Qty</span></div>';
        for (const [price, qty] of asks.slice(0, 8)) {
            const pct = (qty / maxQty * 100).toFixed(0);
            const mine = myAsk.has(price) ? ' mine' : '';
            html += '<div class="ob-row ask-row' + mine +
                '"><div class="depth-bar" style="width:' + pct +
                '%"></div><span class="price">' + price +
                'c</span><span class="qty">' + qty + '</span></div>';
        }
        html += '</div></div>';
        html += '<div class="ob-spread">Spread: ' + (bestAsk - bestBid) +
            'c | Best bid: ' + bestBid + 'c | Best ask: ' + bestAsk + 'c</div>';
        html += '</div>';
    }
    container.innerHTML = html;
}

// ── O/U Trading ──────────────────────────────────────────

function ouGetState() {
    let homeMaps = 0, awayMaps = 0, mapsPlayed = 0;
    for (let i = 0; i < 3; i++) {
        if (mapResults[i] !== 0) {
            mapsPlayed = i + 1;
            if (mapResults[i] === 1) homeMaps++;
            else awayMaps++;
        } else break;
    }
    const ov = document.getElementById('override-prob').value;
    const homeProb = ov ? parseInt(ov) / 100 : currentGame.homeProb;
    return {
        home: currentGame.home,
        away: currentGame.away,
        home_prob: homeProb,
        home_maps: homeMaps,
        away_maps: awayMaps,
        maps_played: mapsPlayed,
        home_rounds: homeRounds,
        away_rounds: awayRounds,
        contracts: parseInt(document.getElementById('ou-contracts').value),
        spread_cents: parseInt(document.getElementById('ou-spread-cents').value),
        ou_tickers: currentGame.ouTickers,
    };
}

async function ouComputeFair() {
    if (!currentGame) return;
    try {
        const resp = await fetch('/api/compute_fair', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(getState()),
        });
        const data = await resp.json();
        if (data.alt_lines && data.alt_lines.over_under) {
            const ou = data.alt_lines.over_under;
            ouOverFair = ou.over_fair;
            document.getElementById('ou-over-fair').textContent =
                'Over 2.5 ' + ou.over_fair + 'c';
            document.getElementById('ou-under-fair').textContent =
                'Under 2.5 ' + ou.under_fair + 'c';
            document.getElementById('ou-pregame-pct').textContent =
                (ou.over_prob * 100).toFixed(1) + '% / ' + (ou.under_prob * 100).toFixed(1) + '%';
            document.getElementById('ou-live-pct').textContent =
                ou.over_fair + 'c / ' + ou.under_fair + 'c';
            document.getElementById('ou-score-label').textContent =
                data.home_maps + '-' + data.away_maps +
                (homeRounds || awayRounds ? ' (R' + homeRounds + '-' + awayRounds + ')' : '');
        }
        if (ouAutoRepricing && !ouRepriceInFlight && ouLastPostedFair !== null) {
            if (ouOverFair !== ouLastPostedFair) {
                console.log('[OU-REPRICE] Fair moved ' + ouLastPostedFair + 'c -> ' + ouOverFair + 'c');
                ouRepriceInFlight = true;
                ouDoReprice().finally(() => { ouRepriceInFlight = false; });
            }
        }
    } catch(e) {
        console.error('ouComputeFair error:', e);
    }
}

async function ouDoReprice() {
    try {
        const resp = await fetch('/api/ou_cancel_and_repost', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(ouGetState()),
        });
        const data = await resp.json();
        if (data.over_fair !== undefined) {
            ouLastPostedFair = data.over_fair;
            ouOverFair = data.over_fair;
        }
        ouRenderResults(data);
    } catch(e) {
        console.error('ouDoReprice error:', e);
    }
}

function ouUpdateRepriceIndicator() {
    const el = document.getElementById('ou-reprice-status');
    if (el) {
        if (ouAutoRepricing) {
            el.textContent = 'AUTO-REPRICE ON';
            el.style.display = 'inline-block';
            el.style.background = '#4caf50';
            el.style.color = '#fff';
            el.style.padding = '2px 8px';
            el.style.borderRadius = '4px';
            el.style.fontSize = '11px';
            el.style.fontWeight = '700';
        } else {
            el.textContent = '';
            el.style.display = 'none';
        }
    }
}

function ouShowLoading() { document.getElementById('ou-loading').style.display = 'block'; }
function ouHideLoading() { document.getElementById('ou-loading').style.display = 'none'; }

function ouRenderResults(data) {
    if (data.error) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">' + data.error + '</div>';
        return;
    }
    let html = '';
    if (data.cancelled)
        html += '<div class="alert alert-info">Cancelled existing orders first</div>';
    if (data.message)
        html += '<div class="alert alert-success">' + data.message + '</div>';

    if (data.over_fair !== undefined) {
        document.getElementById('ou-over-fair').textContent =
            'Over 2.5 ' + data.over_fair + 'c';
        document.getElementById('ou-under-fair').textContent =
            'Under 2.5 ' + data.under_fair + 'c';
        ouOverFair = data.over_fair;
    }

    if (data.orders && data.orders.length > 0) {
        html += '<table class="orders-table"><thead><tr>' +
            '<th>Selection</th><th>Price</th><th>Qty</th><th>Status</th>' +
            '</tr></thead><tbody>';
        for (const o of data.orders) {
            const cls = (o.status === 'placed' || o.status === 'filled')
                ? 'placed' : 'failed';
            let st = o.status.toUpperCase();
            if (o.filled !== undefined) st = o.filled + '/' + o.contracts + ' FILLED';
            html += '<tr><td>' + o.team + '</td><td>' + o.price + 'c</td><td>' +
                o.contracts + '</td><td class="status-' + cls + '">' + st + '</td></tr>';
        }
        html += '</tbody></table>';
    }
    document.getElementById('ou-results').innerHTML = html;
}

async function ouStartAutoReprice() {
    if (!currentGame) return;
    if (ouAutoRepricing) {
        await ouPostOrders();
        return;
    }
    ouAutoRepricing = true;
    ouUpdateRepriceIndicator();
    console.log('[OU-REPRICE] Auto-reprice STARTED');
    await ouPostOrders();
}

function ouStopAutoReprice() {
    const wasActive = ouAutoRepricing;
    ouAutoRepricing = false;
    ouLastPostedFair = null;
    ouRepriceInFlight = false;
    ouUpdateRepriceIndicator();
    if (wasActive) console.log('[OU-REPRICE] Auto-reprice STOPPED');
    ouCancelAll();
}

async function ouPostOrders() {
    if (!currentGame) return;
    ouShowLoading();
    try {
        const resp = await fetch('/api/ou_compute_and_post', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(ouGetState()),
        });
        const data = await resp.json();
        ouRenderResults(data);
        if (ouAutoRepricing && data.over_fair !== undefined) {
            ouLastPostedFair = data.over_fair;
        }
    } catch(e) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    ouHideLoading();
}

async function ouPostIOC() {
    if (!currentGame) return;
    ouShowLoading();
    try {
        const resp = await fetch('/api/ou_compute_and_ioc', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(ouGetState()),
        });
        ouRenderResults(await resp.json());
    } catch(e) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    ouHideLoading();
}

async function ouCancelRepost() {
    if (!currentGame) return;
    ouShowLoading();
    try {
        const resp = await fetch('/api/ou_cancel_and_repost', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(ouGetState()),
        });
        ouRenderResults(await resp.json());
    } catch(e) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    ouHideLoading();
}

async function ouCancelAll() {
    ouShowLoading();
    try {
        const ouTickers = currentGame ? currentGame.ouTickers : [];
        const resp = await fetch('/api/cancel_all', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers: ouTickers}),
        });
        const data = await resp.json();
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-success">' + data.message + '</div>';
    } catch(e) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    ouHideLoading();
}

async function ouJumpBid() {
    if (!currentGame || !currentGame.ouTickers.length) return;
    const sel = JSON.parse(document.getElementById('ou-jump-team').value);
    const contracts = parseInt(document.getElementById('ou-contracts').value);
    ouShowLoading();
    try {
        const resp = await fetch('/api/jump_bid', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ticker: sel.ticker, side: sel.side, contracts}),
        });
        ouRenderResults(await resp.json());
    } catch(e) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    ouHideLoading();
}

async function ouManualBuy() { await ouManualOrder(false); }
async function ouManualIOC() { await ouManualOrder(true); }

async function ouManualOrder(ioc) {
    if (!currentGame || !currentGame.ouTickers.length) return;
    const sel = JSON.parse(document.getElementById('ou-manual-team').value);
    const price = parseInt(document.getElementById('ou-manual-price').value);
    const contracts = parseInt(document.getElementById('ou-contracts').value);
    if (!price || price < 1 || price > 99) { alert('Enter price 1-99c'); return; }
    ouShowLoading();
    try {
        const resp = await fetch('/api/manual_order', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                ticker: sel.ticker, side: sel.side, price, contracts, ioc}),
        });
        ouRenderResults(await resp.json());
    } catch(e) {
        document.getElementById('ou-results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    ouHideLoading();
}

// ── O/U Orderbook ────────────────────────────────────────────
function ouStartOB() { ouStopOB(); ouFetchOB(); ouObInterval = setInterval(ouFetchOB, 3000); }
function ouStopOB() { if (ouObInterval) { clearInterval(ouObInterval); ouObInterval = null; } }

async function ouFetchOB() {
    if (!currentGame || !currentGame.ouTickers.length) return;
    try {
        const resp = await fetch('/api/orderbook', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers: currentGame.ouTickers}),
        });
        const data = await resp.json();
        renderOB(data.books, 'ou-orderbook-content');
        document.getElementById('ou-ob-last-update').textContent =
            new Date().toLocaleTimeString();
    } catch(e) {}
}

// ── Forfeit Iceberg ──────────────────────────────────────
function ffPopulateTeams() {
    const el = document.getElementById('ff-team');
    el.innerHTML = '';
    if (!currentGame) return;
    const mtype = document.getElementById('ff-market-type').value;
    const tickers = mtype === 'totalmaps' ? currentGame.ouTickers : currentGame.tickers;
    if (!tickers || !tickers.length) return;
    const seen = new Set();
    for (const t of tickers) {
        if (!seen.has(t.yes_team)) {
            seen.add(t.yes_team);
            const o = document.createElement('option');
            o.value = JSON.stringify({ticker: t.ticker, side: 'yes'});
            o.textContent = t.yes_team;
            el.appendChild(o);
        }
        if (!seen.has(t.no_team)) {
            seen.add(t.no_team);
            const o = document.createElement('option');
            o.value = JSON.stringify({ticker: t.ticker, side: 'no'});
            o.textContent = t.no_team;
            el.appendChild(o);
        }
    }
    if (mtype === 'totalmaps') {
        el.value = JSON.stringify({ticker: tickers[0].ticker, side: 'no'});
    }
}

function ffStartOB() { ffStopOB(); ffFetchOB(); ffObInterval = setInterval(ffFetchOB, 3000); }
function ffStopOB() { if (ffObInterval) { clearInterval(ffObInterval); ffObInterval = null; } }

async function ffFetchOB() {
    if (!currentGame) return;
    const mtype = document.getElementById('ff-market-type').value;
    const tickers = mtype === 'totalmaps' ? currentGame.ouTickers : currentGame.tickers;
    if (!tickers || !tickers.length) return;
    try {
        const resp = await fetch('/api/orderbook', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers}),
        });
        const data = await resp.json();
        renderOB(data.books, 'ff-orderbook-content');
        document.getElementById('ff-ob-last-update').textContent =
            new Date().toLocaleTimeString();
    } catch(e) {}
}

function ffLog(msg) {
    const el = document.getElementById('ff-log');
    const ts = new Date().toLocaleTimeString();
    el.innerHTML = '<div style="margin-bottom:2px;"><span style="color:#666;">' +
        ts + '</span> ' + msg + '</div>' + el.innerHTML;
}

function ffUpdateStats() {
    document.getElementById('ff-filled-count').textContent = ffTotalFilled;
    const avg = ffTotalFilled > 0 ? (ffTotalCost / ffTotalFilled).toFixed(1) : '—';
    document.getElementById('ff-avg-price').textContent = avg + (ffTotalFilled > 0 ? 'c' : '');
    document.getElementById('ff-total-cost').textContent = '$' + (ffTotalCost / 100).toFixed(2);
    document.getElementById('ff-orders-sent').textContent = ffOrdersSent;
}

function ffUpdateStatus() {
    const el = document.getElementById('ff-status');
    if (ffActive) {
        const delay = parseInt(document.getElementById('ff-delay').value) || 30;
        el.textContent = 'ICEBERG ACTIVE — every ' + delay + 's';
        el.style.display = 'inline-block';
        el.style.background = '#4caf50';
        el.style.color = '#fff';
        el.style.padding = '2px 8px';
        el.style.borderRadius = '4px';
        el.style.fontSize = '11px';
        el.style.fontWeight = '700';
    } else {
        el.textContent = '';
        el.style.display = 'none';
    }
}

async function ffFireOrder() {
    if (!ffActive || !currentGame) return;
    const sel = document.getElementById('ff-team');
    if (!sel.value) return;
    const {ticker, side} = JSON.parse(sel.value);
    const maxPrice = parseInt(document.getElementById('ff-price').value) || 97;
    const count = Math.floor(Math.random() * 5) + 1;

    try {
        const resp = await fetch('/api/forfeit/ioc', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ticker, side, max_price: maxPrice, count}),
        });
        const data = await resp.json();
        ffOrdersSent++;
        if (data.filled > 0) {
            ffTotalFilled += data.filled;
            ffTotalCost += data.filled * data.fill_price;
            ffLog('<span style="color:#4caf50;">FILLED ' + data.filled + '/' +
                count + ' @ ' + data.fill_price + 'c</span>');
        } else if (data.no_ask) {
            ffLog('<span style="color:#666;">No ask available</span>');
        } else {
            ffLog('<span style="color:#ff9800;">NOT FILLED ' + count +
                ' @ ' + data.tried_price + 'c (ask ' + (data.best_ask || '?') + 'c)</span>');
        }
        ffUpdateStats();
    } catch(e) {
        ffLog('<span style="color:#ef5350;">ERROR: ' + e.message + '</span>');
    }

    if (ffActive) {
        const delay = parseInt(document.getElementById('ff-delay').value) || 30;
        ffTimeout = setTimeout(ffFireOrder, delay * 1000);
    }
}

function ffStart() {
    if (!currentGame) return;
    ffActive = true;
    ffUpdateStatus();
    document.getElementById('ff-start-btn').style.display = 'none';
    document.getElementById('ff-stop-btn').style.display = '';
    document.getElementById('ff-stats').style.display = 'block';
    ffLog('<b>Iceberg started</b>');
    ffFireOrder();
}

function ffStop() {
    ffActive = false;
    if (ffTimeout) { clearTimeout(ffTimeout); ffTimeout = null; }
    ffUpdateStatus();
    document.getElementById('ff-start-btn').style.display = '';
    document.getElementById('ff-stop-btn').style.display = 'none';
    ffLog('<b>Iceberg stopped</b>');
}

// ── HLTV Scoreboard ───────────────────────────────────────
let sbInterval = null;

function onGameSelectSB() {
    const section = document.getElementById('sb-section');
    section.style.display = currentGame ? 'block' : 'none';
    if (currentGame) refreshHLTVList();
}

async function refreshHLTVList() {
    try {
        const resp = await fetch('/api/hltv_live');
        const data = await resp.json();
        const sel = document.getElementById('hltv-select');
        const prev = sel.value;
        sel.innerHTML = '<option value="">-- HLTV Live Matches --</option>';
        for (const m of data.matches || []) {
            const o = document.createElement('option');
            o.value = JSON.stringify(m);
            o.textContent = m.team1 + ' vs ' + m.team2 + ' (' + m.format + ')';
            sel.appendChild(o);
        }
        if (prev) sel.value = prev;
    } catch(e) { console.error('HLTV list error:', e); }
}

async function watchHLTV() {
    const sel = document.getElementById('hltv-select');
    if (!sel.value) return;
    const m = JSON.parse(sel.value);
    try {
        await fetch('/api/watch_match', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({match_id: m.match_id, url: m.url}),
        });
        document.getElementById('sb-status').textContent = 'Connecting...';
        document.getElementById('sb-status').className = 'sb-status';
        document.getElementById('sb-board').style.display = 'block';
        startSB();
    } catch(e) { console.error('Watch error:', e); }
}

async function stopHLTV() {
    stopSB();
    try { await fetch('/api/stop_watch', {method: 'POST'}); } catch(e) {}
    document.getElementById('sb-status').textContent = 'Not connected';
    document.getElementById('sb-status').className = 'sb-status';
    document.getElementById('sb-board').style.display = 'none';
}

function startSB() { stopSB(); fetchSB(); sbInterval = setInterval(fetchSB, 1000); }
function stopSB() { if (sbInterval) { clearInterval(sbInterval); sbInterval = null; } }

async function fetchSB() {
    try {
        const resp = await fetch('/api/scoreboard');
        const data = await resp.json();
        if (data.state) {
            renderScoreboard(data.state);
            document.getElementById('sb-status').textContent = 'Live';
            document.getElementById('sb-status').className = 'sb-status connected';
            autoUpdateFromScoreboard(data.state);
        } else {
            document.getElementById('sb-status').textContent = data.watching ? 'Loading...' : 'Not connected';
            document.getElementById('sb-status').className = 'sb-status';
        }
    } catch(e) {}
}

function renderScoreboard(s) {
    const board = document.getElementById('sb-board');
    let html = '';
    html += '<div class="sb-header">';
    html += '<span class="sb-round-num">Round ' + s.round + '</span>';
    html += '<span class="sb-map-info">' + (s.map || '') + '</span>';
    html += '<span class="sb-timer">' + (s.timer || '') + '</span>';
    html += '</div>';

    html += renderTeamBlock(s.ct_players, 'ct', s.ct_team, s.ct_score);
    html += renderTeamBlock(s.t_players, 't', s.t_team, s.t_score);
    board.innerHTML = html;
}

function renderTeamBlock(players, side, teamName, score) {
    let html = '<div class="sb-team-header ' + side + '">';
    html += '<span>' + (teamName || side.toUpperCase()) + '</span>';
    html += '<span class="sb-team-score">' + (score || 0) + '</span>';
    html += '</div>';

    html += '<div class="sb-player-header">';
    html += '<span></span><span>Player</span><span>HP</span><span></span><span></span>';
    html += '<span style="text-align:right">$</span>';
    html += '<span style="text-align:center">K</span>';
    html += '<span style="text-align:center">A</span>';
    html += '<span style="text-align:center">D</span>';
    html += '<span style="text-align:center">ADR</span>';
    html += '</div>';

    for (const p of (players || [])) {
        const dead = p.alive ? '' : ' dead';
        const hpClass = p.hp > 60 ? 'hp-high' : p.hp > 25 ? 'hp-mid' : 'hp-low';
        html += '<div class="sb-player-row' + dead + '">';

        if (p.weapon_img) {
            html += '<img class="sb-weapon-icon" src="' + p.weapon_img + '" alt="">';
        } else {
            html += '<span></span>';
        }

        html += '<span class="sb-player-name">' + (p.name || '') + '</span>';

        html += '<span style="display:flex;align-items:center;gap:4px;">';
        html += '<span class="sb-hp-bar-outer"><span class="sb-hp-bar-inner ' + hpClass +
            '" style="width:' + p.hp + '%"></span></span>';
        html += '<span class="sb-hp-text">' + p.hp + '</span>';
        html += '</span>';

        if (p.armor_img) {
            html += '<img class="sb-armor-icon" src="' + p.armor_img + '" alt="">';
        } else {
            html += '<span></span>';
        }

        if (p.has_defuse) {
            html += '<img class="sb-defuse-icon" src="https://www.hltv.org/img/static/scoreboard/weapons/defusekit.png" alt="kit">';
        } else {
            html += '<span></span>';
        }

        html += '<span class="sb-money">$' + (p.money || 0).toLocaleString() + '</span>';
        html += '<span class="sb-stat kills">' + (p.kills || 0) + '</span>';
        html += '<span class="sb-stat">' + (p.assists || 0) + '</span>';
        html += '<span class="sb-stat deaths">' + (p.deaths || 0) + '</span>';
        html += '<span class="sb-stat">' + (p.adr || 0).toFixed(0) + '</span>';
        html += '</div>';
    }
    return html;
}

function autoUpdateFromScoreboard(s) {
    if (!currentGame) return;
    const homeLower = currentGame.home.toLowerCase();
    const awayLower = currentGame.away.toLowerCase();
    const ctLower = (s.ct_team || '').toLowerCase();
    const tLower = (s.t_team || '').toLowerCase();

    let homeIsCtSide = null;
    if (ctLower.includes(homeLower) || homeLower.includes(ctLower)) homeIsCtSide = true;
    else if (tLower.includes(homeLower) || homeLower.includes(tLower)) homeIsCtSide = false;
    else if (ctLower.includes(awayLower) || awayLower.includes(ctLower)) homeIsCtSide = false;
    else if (tLower.includes(awayLower) || awayLower.includes(tLower)) homeIsCtSide = true;

    if (homeIsCtSide === null) return;

    const newHomeRounds = homeIsCtSide ? s.ct_score : s.t_score;
    const newAwayRounds = homeIsCtSide ? s.t_score : s.ct_score;

    if (newHomeRounds !== homeRounds || newAwayRounds !== awayRounds) {
        homeRounds = newHomeRounds;
        awayRounds = newAwayRounds;
        document.getElementById('home-rounds').textContent = homeRounds;
        document.getElementById('away-rounds').textContent = awayRounds;
    }
    // Always recompute — economy/weapons change mid-round
    computeFair();
    ouComputeFair();
}

// patch onGameSelect to also trigger scoreboard
const _origOnGameSelect = onGameSelect;
onGameSelect = function() { _origOnGameSelect(); onGameSelectSB(); };

// ── Keyboard shortcuts ─────────────────────────────────────
document.addEventListener('keydown', function(e) {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' ||
        e.target.tagName === 'TEXTAREA') return;
    const key = e.key.toUpperCase();
    if (activeTab === 'ou-trade') {
        if (key === 'C') ouComputeFair();
        else if (key === 'P') ouStartAutoReprice();
        else if (key === 'I') ouPostIOC();
        else if (key === 'R') ouCancelRepost();
        else if (key === 'X') ouStopAutoReprice();
        else if (key === 'J') ouJumpBid();
        else if (key === 'B') ouManualBuy();
    } else {
        if (key === 'C') computeFair();
        else if (key === 'P') startAutoReprice();
        else if (key === 'I') postIOC();
        else if (key === 'R') cancelRepost();
        else if (key === 'X') stopAutoReprice();
        else if (key === 'J') jumpBid();
        else if (key === 'B') manualBuy();
        else if (key === 'A') toggleAutoTrade();
    }
});

// ── Tab switching ─────────────────────────────────────────
function switchTab(tab) {
    activeTab = tab;
    document.getElementById('tab-trade').style.display = tab === 'trade' ? '' : 'none';
    document.getElementById('tab-esports').style.display = tab === 'esports' ? '' : 'none';
    document.getElementById('tab-ou-trade').style.display = tab === 'ou-trade' ? '' : 'none';
    document.getElementById('tab-predict').style.display = tab === 'predict' ? '' : 'none';
    document.getElementById('tab-bracket').style.display = tab === 'bracket' ? '' : 'none';
    document.getElementById('tab-futures').style.display = tab === 'futures' ? '' : 'none';
    document.getElementById('tab-forfeit').style.display = tab === 'forfeit' ? '' : 'none';
    document.getElementById('tab-screen').style.display = tab === 'screen' ? '' : 'none';
    const showMatch = (tab === 'trade' || tab === 'ou-trade' || tab === 'forfeit');
    document.getElementById('match-state').style.display = showMatch ? 'block' : 'none';
    document.querySelectorAll('.tab-bar .tab').forEach(b => b.classList.remove('active'));
    event.target.classList.add('active');
    if (tab === 'predict' && !document.getElementById('team-list-a').children.length) {
        fetch('/api/team_list').then(r => r.json()).then(data => {
            const listA = document.getElementById('team-list-a');
            const listB = document.getElementById('team-list-b');
            data.teams.forEach(t => {
                listA.innerHTML += '<option value="' + t + '">';
                listB.innerHTML += '<option value="' + t + '">';
            });
        });
    }
    if (tab === 'bracket') {
        brkRefreshSaves();
        if (BRK_ALL_TEAMS.length === 0) {
            fetch('/api/team_list').then(r => r.json()).then(data => {
                BRK_ALL_TEAMS = data.teams;
                brkRender();
            });
        }
    }
    if (tab === 'forfeit' && currentGame && (currentGame.tickers.length || currentGame.ouTickers.length)) {
        ffStartOB();
    } else {
        ffStopOB();
    }
}

function runPredict() {
    const a = document.getElementById('pred-team-a').value.trim();
    const b = document.getElementById('pred-team-b').value.trim();
    const format = document.getElementById('pred-format').value;
    if (!a || !b) return;
    fetch('/api/predict', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({team_a: a, team_b: b, format: format})
    }).then(r => r.json()).then(data => {
        if (data.error) { alert(data.error); return; }
        document.getElementById('pred-a-name').textContent = data.team_a;
        document.getElementById('pred-b-name').textContent = data.team_b;
        document.getElementById('pred-a-pct').textContent = (data.prob_a * 100).toFixed(1) + '%';
        document.getElementById('pred-b-pct').textContent = (data.prob_b * 100).toFixed(1) + '%';
        document.getElementById('pred-a-ml').textContent = data.ml_a;
        document.getElementById('pred-b-ml').textContent = data.ml_b;
        document.getElementById('pred-a-games').textContent = data.team_a + ': ' + data.games_a + ' games';
        document.getElementById('pred-b-games').textContent = data.team_b + ': ' + data.games_b + ' games';
        const ouRow = document.getElementById('pred-ou-row');
        if (data.over_under) {
            const ou = data.over_under;
            ouRow.innerHTML =
                '<div class="ou-chip"><div class="ou-label">Over ' + ou.line + '</div>' +
                '<div class="ou-pct">' + (ou.over_prob * 100).toFixed(1) + '%</div>' +
                '<div class="ou-fair">' + ou.over_fair + 'c</div></div>' +
                '<div class="ou-chip"><div class="ou-label">Under ' + ou.line + '</div>' +
                '<div class="ou-pct">' + (ou.under_prob * 100).toFixed(1) + '%</div>' +
                '<div class="ou-fair">' + ou.under_fair + 'c</div></div>';
            ouRow.style.display = '';
        } else {
            ouRow.innerHTML = '';
            ouRow.style.display = 'none';
        }
        document.getElementById('pred-result').style.display = '';
    });
}

// ── Bracket Builder ────────────────────────────────────────────
let BRK_ALL_TEAMS = [];
let brkRounds = [];
let swissMode = false;
let swissStages = [];
let brkActiveName = null;

function brkAutosave() {
    if (!brkActiveName) return;
    const saves = JSON.parse(localStorage.getItem('cs2_brackets') || '{}');
    saves[brkActiveName] = {
        swissStages: JSON.parse(JSON.stringify(swissStages)),
        brkRounds: JSON.parse(JSON.stringify(brkRounds)),
    };
    localStorage.setItem('cs2_brackets', JSON.stringify(saves));
}

function brkClear() {
    brkRounds = [];
    swissMode = false;
    swissStages = [];
    document.getElementById('swiss-panel').style.display = 'none';
    document.getElementById('brk-bracket-area').style.display = '';
    brkRender();
    document.getElementById('bracket-results').innerHTML = '';
}

function brkAddRound() {
    brkRounds.push({label: 'Round ' + (brkRounds.length + 1), games: []});
    brkRender();
    brkAutosave();
}

function brkRemoveRound(ri) {
    brkRounds.splice(ri, 1);
    for (let r = 0; r < brkRounds.length; r++) {
        for (const g of brkRounds[r].games) {
            for (const sk of ['slot1','slot2']) {
                const s = g[sk];
                if ((s.type === 'winner' || s.type === 'loser') && s.round >= ri) {
                    if (s.round === ri) { g[sk] = {type:'team', team: BRK_ALL_TEAMS[0]}; }
                    else { s.round--; }
                }
            }
        }
    }
    brkRender();
    brkAutosave();
}

function brkRenameRound(ri) {
    const name = prompt('Round name:', brkRounds[ri].label);
    if (name !== null && name.trim()) { brkRounds[ri].label = name.trim(); brkRender(); brkAutosave(); }
}

function brkAddGame(ri) {
    brkRounds[ri].games.push({
        slot1: {type:'team', team: BRK_ALL_TEAMS[0]},
        slot2: {type:'team', team: BRK_ALL_TEAMS[0]},
        bo: 3,
    });
    brkRender();
    brkAutosave();
}

function brkRemoveGame(ri, gi) {
    brkRounds[ri].games.splice(gi, 1);
    for (let r = ri + 1; r < brkRounds.length; r++) {
        for (const g of brkRounds[r].games) {
            for (const sk of ['slot1','slot2']) {
                const s = g[sk];
                if ((s.type === 'winner' || s.type === 'loser') && s.round === ri) {
                    if (s.game === gi) { g[sk] = {type:'team', team: BRK_ALL_TEAMS[0]}; }
                    else if (s.game > gi) { s.game--; }
                }
            }
        }
    }
    brkRender();
    brkAutosave();
}

function brkSlotChange(ri, gi, sk, val) {
    if (val.startsWith('SA:')) {
        const ref = val.split(':')[1].split(',');
        brkRounds[ri].games[gi][sk] = {type:'swiss_advance', stage: parseInt(ref[0]), seed: parseInt(ref[1])};
    } else if (val.startsWith('W:') || val.startsWith('L:')) {
        const parts = val.split(':');
        const type = parts[0] === 'W' ? 'winner' : 'loser';
        const ref = parts[1].split(',');
        brkRounds[ri].games[gi][sk] = {type, round: parseInt(ref[0]), game: parseInt(ref[1])};
    } else {
        brkRounds[ri].games[gi][sk] = {type:'team', team: val};
    }
    brkAutosave();
}

function brkSlotVal(slot) {
    if (slot.type === 'team') return slot.team;
    if (slot.type === 'swiss_advance') return 'SA:' + slot.stage + ',' + slot.seed;
    const prefix = slot.type === 'winner' ? 'W' : 'L';
    return prefix + ':' + slot.round + ',' + slot.game;
}

function brkSlotLabel(slot) {
    if (slot.type === 'team') return slot.team;
    if (slot.type === 'swiss_advance') {
        const sLabel = slot.stage < swissStages.length ? swissStages[slot.stage].label : 'Stage '+(slot.stage+1);
        return 'Adv. ' + sLabel + ' #' + (slot.seed + 1);
    }
    const prefix = slot.type === 'winner' ? 'W' : 'L';
    const rLabel = slot.round < brkRounds.length ? brkRounds[slot.round].label : 'R'+(slot.round+1);
    return prefix + ' ' + rLabel + ' G' + (slot.game + 1);
}

function brkRender() {
    const area = document.getElementById('brk-bracket-area');
    let html = '';
    for (let ri = 0; ri < brkRounds.length; ri++) {
        const rnd = brkRounds[ri];
        html += '<div class="brk-round">';
        html += '<div class="brk-round-hdr">';
        html += '<span class="round-label" ondblclick="brkRenameRound('+ri+')" title="Double-click to rename">'+rnd.label+'</span>';
        html += '<div class="round-actions">';
        html += '<button class="round-action" onclick="brkAddGame('+ri+')" title="Add game">+G</button>';
        html += '<button class="round-action danger" onclick="brkRemoveRound('+ri+')" title="Remove round">&times;</button>';
        html += '</div></div>';

        for (let gi = 0; gi < rnd.games.length; gi++) {
            const game = rnd.games[gi];
            html += '<div class="brk-game">';
            const bo = game.bo || 3;
            html += '<div class="brk-game-num">G'+(gi+1);
            html += ' <select onchange="brkRounds['+ri+'].games['+gi+'].bo=parseInt(this.value);brkAutosave()" style="background:#111;color:#4fc3f7;border:1px solid #333;border-radius:3px;font-size:9px;padding:0 2px;">';
            html += '<option value="1"'+(bo===1?' selected':'')+'>BO1</option>';
            html += '<option value="3"'+(bo===3?' selected':'')+'>BO3</option>';
            html += '<option value="5"'+(bo===5?' selected':'')+'>BO5</option>';
            html += '</select>';
            html += '<button class="round-action danger" onclick="brkRemoveGame('+ri+','+gi+')" style="margin-left:4px;font-size:9px;" title="Remove game">&times;</button></div>';
            html += brkSlotSelect(ri, gi, 'slot1', game.slot1);
            html += brkSlotSelect(ri, gi, 'slot2', game.slot2);
            if (ri < brkRounds.length - 1) html += '<div class="brk-game-connector"></div>';
            html += '</div>';
        }
        if (rnd.games.length === 0) {
            html += '<div style="color:#555;font-size:12px;text-align:center;padding:10px;">No games &mdash; click +G</div>';
        }
        html += '</div>';
    }
    html += '<div class="brk-add-round"><button onclick="brkAddRound()">+ Round</button></div>';
    area.innerHTML = html;
}

function brkSlotSelect(ri, gi, sk, slot) {
    const curVal = brkSlotVal(slot);
    let html = '<div class="brk-slot"><select onchange="brkSlotChange('+ri+','+gi+',\\''+sk+'\\',this.value)">';
    for (const t of BRK_ALL_TEAMS) {
        const sel = (slot.type === 'team' && slot.team === t) ? ' selected' : '';
        html += '<option class="opt-team" value="'+t+'"'+sel+'>'+t+'</option>';
    }
    for (let pr = 0; pr < ri; pr++) {
        for (let pg = 0; pg < brkRounds[pr].games.length; pg++) {
            const wVal = 'W:'+pr+','+pg;
            const lVal = 'L:'+pr+','+pg;
            const wLabel = 'W '+brkRounds[pr].label+' G'+(pg+1);
            const lLabel = 'L '+brkRounds[pr].label+' G'+(pg+1);
            const wSel = curVal === wVal ? ' selected' : '';
            const lSel = curVal === lVal ? ' selected' : '';
            html += '<option class="opt-winner" value="'+wVal+'"'+wSel+'>'+wLabel+'</option>';
            html += '<option class="opt-winner" value="'+lVal+'"'+lSel+' style="color:#ff8a65">'+lLabel+'</option>';
        }
    }
    for (let si = 0; si < swissStages.length; si++) {
        const stLabel = swissStages[si].label;
        const nSlots = swissStages[si].slots.length;
        for (let seed = 0; seed < nSlots; seed++) {
            const saVal = 'SA:'+si+','+seed;
            const saLabel = 'Adv. '+stLabel+' #'+(seed+1);
            const saSel = curVal === saVal ? ' selected' : '';
            html += '<option class="opt-winner" value="'+saVal+'"'+saSel+' style="color:#81c784">'+saLabel+'</option>';
        }
    }
    html += '</select></div>';
    return html;
}

function brkSeedOrder(n) {
    if (n <= 1) return [1];
    if (n === 2) return [1, 2];
    const half = brkSeedOrder(n / 2);
    const r = [];
    for (const s of half) { r.push(s); r.push(n + 1 - s); }
    return r;
}

function brkPreset(key) {
    if (BRK_ALL_TEAMS.length === 0) { alert('Teams not loaded yet'); return; }
    const hadSwiss = swissStages.length > 0;
    brkRounds = [];
    const T = BRK_ALL_TEAMS;
    if (key === '4se') {
        const o = brkSeedOrder(4);
        brkRounds = [
            {label:'Semis', games:[
                {slot1:{type:'team',team:T[o[0]-1]}, slot2:{type:'team',team:T[o[1]-1]}},
                {slot1:{type:'team',team:T[o[2]-1]}, slot2:{type:'team',team:T[o[3]-1]}},
            ]},
            {label:'Final', games:[
                {slot1:{type:'winner',round:0,game:0}, slot2:{type:'winner',round:0,game:1}},
            ]},
        ];
    } else if (key === '8se') {
        const o = brkSeedOrder(8);
        brkRounds = [
            {label:'Quarters', games: []},
            {label:'Semis', games: []},
            {label:'Final', games: []},
        ];
        for (let i = 0; i < 8; i += 2)
            brkRounds[0].games.push({slot1:{type:'team',team:T[o[i]-1]||T[0]}, slot2:{type:'team',team:T[o[i+1]-1]||T[0]}});
        for (let i = 0; i < 4; i += 2)
            brkRounds[1].games.push({slot1:{type:'winner',round:0,game:i}, slot2:{type:'winner',round:0,game:i+1}});
        brkRounds[2].games.push({slot1:{type:'winner',round:1,game:0}, slot2:{type:'winner',round:1,game:1}});
    } else if (key === '8de') {
        const o = brkSeedOrder(8);
        brkRounds = [
            {label:'WR1', games: []},
            {label:'LR1', games: []},
            {label:'WR2', games: []},
            {label:'LR2', games: []},
            {label:'WR Final', games: []},
            {label:'LR3', games: []},
            {label:'LR Final', games: []},
            {label:'Grand Final', games: []},
        ];
        for (let i = 0; i < 8; i += 2)
            brkRounds[0].games.push({slot1:{type:'team',team:T[o[i]-1]||T[0]}, slot2:{type:'team',team:T[o[i+1]-1]||T[0]}});
        brkRounds[1].games.push({slot1:{type:'loser',round:0,game:0}, slot2:{type:'loser',round:0,game:3}});
        brkRounds[1].games.push({slot1:{type:'loser',round:0,game:1}, slot2:{type:'loser',round:0,game:2}});
        brkRounds[2].games.push({slot1:{type:'winner',round:0,game:0}, slot2:{type:'winner',round:0,game:1}});
        brkRounds[2].games.push({slot1:{type:'winner',round:0,game:2}, slot2:{type:'winner',round:0,game:3}});
        brkRounds[3].games.push({slot1:{type:'winner',round:1,game:0}, slot2:{type:'loser',round:2,game:0}});
        brkRounds[3].games.push({slot1:{type:'winner',round:1,game:1}, slot2:{type:'loser',round:2,game:1}});
        brkRounds[4].games.push({slot1:{type:'winner',round:2,game:0}, slot2:{type:'winner',round:2,game:1}});
        brkRounds[5].games.push({slot1:{type:'winner',round:3,game:0}, slot2:{type:'winner',round:3,game:1}});
        brkRounds[6].games.push({slot1:{type:'winner',round:5,game:0}, slot2:{type:'loser',round:4,game:0}});
        brkRounds[7].games.push({slot1:{type:'winner',round:4,game:0}, slot2:{type:'winner',round:6,game:0}});
    } else if (key === '12se4') {
        const o = brkSeedOrder(8);
        brkRounds = [
            {label:'Play-In', games:[]},
            {label:'Quarters', games:[]},
            {label:'Semis', games:[]},
            {label:'Final', games:[]},
        ];
        for (let k = 0; k < 4; k++)
            brkRounds[0].games.push({slot1:{type:'team',team:T[4+k]||T[0]}, slot2:{type:'team',team:T[11-k]||T[0]}});
        for (let i = 0; i < 8; i += 2) {
            const s1 = o[i], s2 = o[i+1];
            const mk = (s) => s <= 4 ? {type:'team',team:T[s-1]||T[0]} : {type:'winner',round:0,game:s-5};
            brkRounds[1].games.push({slot1:mk(s1), slot2:mk(s2)});
        }
        for (let i = 0; i < 4; i += 2)
            brkRounds[2].games.push({slot1:{type:'winner',round:1,game:i}, slot2:{type:'winner',round:1,game:i+1}});
        brkRounds[3].games.push({slot1:{type:'winner',round:2,game:0}, slot2:{type:'winner',round:2,game:1}});
    } else if (key === 'swiss') {
        swissMode = true;
        brkRounds = [];
        swissStages = [{label:'Stage 1', winsToAdv:3, lossesToElim:3, slots:[], midStage:false, matchBo:1, deciderBo:3, nextMatchups:[]}];
        const ct = Math.min(16, T.length);
        for (let i = 0; i < ct; i++) swissStages[0].slots.push({type:'team', team:T[i], wins:0, losses:0});
        document.getElementById('brk-bracket-area').style.display = 'none';
        document.getElementById('swiss-panel').style.display = '';
        swissRender();
        document.getElementById('bracket-results').innerHTML = '';
        brkAutosave();
        return;
    }
    if (hadSwiss && key !== 'swiss') {
        const lastStage = swissStages.length - 1;
        const teamToSeed = {};
        for (let i = 0; i < T.length; i++) teamToSeed[T[i]] = i;
        for (const rnd of brkRounds) {
            for (const game of rnd.games) {
                for (const sk of ['slot1', 'slot2']) {
                    if (game[sk].type === 'team') {
                        const seed = teamToSeed[game[sk].team] || 0;
                        game[sk] = {type: 'swiss_advance', stage: lastStage, seed: seed};
                    }
                }
            }
        }
        document.getElementById('swiss-panel').style.display = '';
        document.getElementById('brk-bracket-area').style.display = '';
        brkRender();
        document.getElementById('bracket-results').innerHTML = '';
        brkAutosave();
        return;
    }
    swissMode = false;
    swissStages = [];
    document.getElementById('swiss-panel').style.display = 'none';
    document.getElementById('brk-bracket-area').style.display = '';
    brkRender();
    document.getElementById('bracket-results').innerHTML = '';
    brkAutosave();
}

async function brkSimulate() {
    if (swissStages.length > 0 && brkRounds.length > 0) {
        const sims = parseInt(document.getElementById('bracket-sims').value) || 100000;
        const payload = {
            rounds: brkRounds.map(r => ({
                label: r.label,
                games: r.games.map(g => ({slot1: g.slot1, slot2: g.slot2, bo: g.bo || 3})),
            })),
            swiss_stages: swissStages.map(st => ({
                label: st.label,
                wins_to_advance: st.winsToAdv,
                losses_to_eliminate: st.lossesToElim,
                mid_stage: st.midStage || false,
                match_bo: st.matchBo || 1,
                decider_bo: st.deciderBo || 3,
                next_matchups: (st.nextMatchups || []).map(m => ({team1: m.team1, team2: m.team2})),
                slots: st.slots.map(s => s.type === 'advance' ? {type:'advance', stage:s.stage} : {type:'team', team:s.team, wins:s.wins||0, losses:s.losses||0}),
            })),
            sims,
        };
        document.getElementById('bracket-loading').style.display = 'block';
        document.getElementById('bracket-results').innerHTML = '';
        try {
            const resp = await fetch('/api/bracket_simulate', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            const data = await resp.json();
            brkRenderResults(data);
        } catch(e) {
            document.getElementById('bracket-results').innerHTML =
                '<div class="alert alert-error">Error: '+e.message+'</div>';
        }
        document.getElementById('bracket-loading').style.display = 'none';
        return;
    }
    if (swissStages.length > 0) { swissSimulate(); return; }
    if (brkRounds.length === 0) { alert('Add rounds and games first'); return; }
    const totalGames = brkRounds.reduce((s, r) => s + r.games.length, 0);
    if (totalGames === 0) { alert('No games in bracket'); return; }

    const sims = parseInt(document.getElementById('bracket-sims').value) || 100000;
    const payload = {
        rounds: brkRounds.map(r => ({
            label: r.label,
            games: r.games.map(g => ({slot1: g.slot1, slot2: g.slot2, bo: g.bo || 3})),
        })),
        sims,
    };

    document.getElementById('bracket-loading').style.display = 'block';
    document.getElementById('bracket-results').innerHTML = '';

    try {
        const resp = await fetch('/api/bracket_simulate', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
        });
        const data = await resp.json();
        brkRenderResults(data);
    } catch(e) {
        document.getElementById('bracket-results').innerHTML =
            '<div class="alert alert-error">Error: '+e.message+'</div>';
    }
    document.getElementById('bracket-loading').style.display = 'none';
}

function brkRenderResults(data) {
    lastBracketSimData = data;
    if (data.error) {
        document.getElementById('bracket-results').innerHTML =
            '<div class="alert alert-error">'+data.error+'</div>';
        return;
    }
    const labels = data.labels;
    const results = data.results;
    let html = '<div style="margin-top:16px;">';

    if (data.swiss_stages) {
        for (const stage of data.swiss_stages) {
            html += '<h2 style="color:#81c784;margin-top:20px;">'+stage.label+'</h2>';
            const advW = stage.wins_to_advance;
            const elimL = stage.losses_to_eliminate;
            const recordKeys = [];
            for (let w = advW; w >= 0; w--) {
                for (let l = 0; l <= elimL; l++) {
                    if (w === advW || l === elimL) recordKeys.push(w+'-'+l);
                }
            }
            html += '<table class="brk-results-table"><thead><tr>';
            html += '<th>Team</th><th style="text-align:right;">Advance %</th>';
            for (const k of recordKeys) html += '<th style="text-align:right;font-size:11px;">'+k+'</th>';
            html += '</tr></thead><tbody>';
            const maxAdv = Math.max(...stage.results.map(r => r.advance_pct));
            for (const r of stage.results) {
                const advPct = (r.advance_pct * 100).toFixed(1);
                const cls = r.advance_pct >= 0.25 ? 'pct-high' : 'pct';
                html += '<tr><td class="team-name">'+r.team+'</td>';
                html += '<td style="text-align:right" class="'+cls+'">'+advPct+'%</td>';
                for (const k of recordKeys) {
                    const val = r.records[k] || 0;
                    const pct = (val * 100).toFixed(1);
                    html += '<td style="text-align:right;font-size:12px;color:'+(parseFloat(pct)>10?'#e0e0e0':'#555')+'">'+pct+'%</td>';
                }
                html += '</tr>';
            }
            html += '</tbody></table>';
        }
        html += '<hr style="border-color:#333;margin:24px 0;">';
    }

    html += '<h2>Advancement Probabilities</h2>';
    html += '<p style="font-size:12px;color:#888;margin-bottom:8px;">'+data.sims.toLocaleString()+' simulations</p>';
    html += '<table class="brk-results-table"><thead><tr><th>Team</th>';
    for (const l of labels) html += '<th style="text-align:right;">'+l+'</th>';
    html += '</tr></thead><tbody>';
    for (const r of results) {
        html += '<tr><td class="team-name">'+r.team+'</td>';
        for (const l of labels) {
            const val = r.rounds[l];
            if (val === 'bye') {
                html += '<td style="text-align:right" class="pct-bye">bye</td>';
            } else {
                const pctNum = val * 100;
                const pct = pctNum.toFixed(1) + '%';
                const cls = pctNum >= 25 ? 'pct-high' : 'pct';
                html += '<td style="text-align:right" class="'+cls+'">'+pct+'</td>';
            }
        }
        html += '</tr>';
    }
    html += '</tbody></table>';

    const champKey = labels[labels.length - 1];
    const champVals = results.map(r => r.rounds[champKey] === 'bye' ? 0 : r.rounds[champKey]);
    const maxChamp = Math.max(...champVals);
    if (maxChamp > 0) {
        html += '<h2 style="margin-top:24px;">Championship Odds</h2>';
        for (let i = 0; i < results.length; i++) {
            const v = champVals[i];
            if (v <= 0) continue;
            const pct = (v * 100).toFixed(1);
            const barW = (v / maxChamp * 100).toFixed(0);
            html += '<div style="display:flex;align-items:center;gap:10px;margin:5px 0;">';
            html += '<span style="min-width:170px;font-size:13px;color:#e0e0e0;">'+results[i].team+'</span>';
            html += '<div class="brk-champ-bar" style="flex:1;max-width:300px;">';
            html += '<div class="brk-champ-bar-fill" style="width:'+barW+'%;"></div></div>';
            html += '<span style="font-size:13px;color:#4fc3f7;font-weight:600;min-width:55px;">'+pct+'%</span>';
            html += '</div>';
        }
    }

    html += '</div>';
    document.getElementById('bracket-results').innerHTML = html;
}

// ── Swiss Format ──────────────────────────────────────────────

function swissAddStage() {
    const idx = swissStages.length;
    swissStages.push({label:'Stage '+(idx+1), winsToAdv:3, lossesToElim:3, slots:[], midStage:false, matchBo:1, deciderBo:3, nextMatchups:[]});
    const ct = Math.min(16, BRK_ALL_TEAMS.length);
    for (let i = 0; i < ct; i++) swissStages[idx].slots.push({type:'team', team:BRK_ALL_TEAMS[i], wins:0, losses:0});
    swissRender();
}

function swissRemoveStage(si) {
    swissStages.splice(si, 1);
    for (const st of swissStages) {
        for (const slot of st.slots) {
            if (slot.type === 'advance' && slot.stage >= si) {
                if (slot.stage === si) { slot.type = 'team'; slot.team = BRK_ALL_TEAMS[0]; delete slot.stage; }
                else slot.stage--;
            }
        }
    }
    swissRender();
}

function swissRenameStage(si) {
    const name = prompt('Stage name:', swissStages[si].label);
    if (name !== null && name.trim()) { swissStages[si].label = name.trim(); swissRender(); }
}

function swissSetTeamCount(si, count) {
    const st = swissStages[si];
    while (st.slots.length < count) st.slots.push({type:'team', team:BRK_ALL_TEAMS[0], wins:0, losses:0});
    while (st.slots.length > count) st.slots.pop();
    swissRender();
}

function swissSlotChange(si, slotIdx, val) {
    if (val.startsWith('ADV:')) {
        const fromStage = parseInt(val.split(':')[1]);
        swissStages[si].slots[slotIdx] = {type:'advance', stage: fromStage};
    } else {
        swissStages[si].slots[slotIdx] = {type:'team', team: val, wins:0, losses:0};
    }
    swissRender();
}

function swissSlotToTeam(si, slotIdx) {
    swissStages[si].slots[slotIdx] = {type:'team', team: BRK_ALL_TEAMS[0], wins:0, losses:0};
    swissRender();
}

function swissSetRecord(si, slotIdx, field, val) {
    swissStages[si].slots[slotIdx][field] = parseInt(val) || 0;
    brkAutosave();
}

function swissToggleMidStage(si) {
    swissStages[si].midStage = !swissStages[si].midStage;
    if (!swissStages[si].midStage) {
        for (const s of swissStages[si].slots) { s.wins = 0; s.losses = 0; }
        swissStages[si].nextMatchups = [];
    }
    swissRender();
}

function swissGetActivePools(si) {
    const st = swissStages[si];
    const pools = {};
    for (const s of st.slots) {
        if (s.type !== 'team') continue;
        if ((s.wins||0) >= st.winsToAdv || (s.losses||0) >= st.lossesToElim) continue;
        const key = (s.wins||0)+'-'+(s.losses||0);
        if (!pools[key]) pools[key] = [];
        pools[key].push(s.team);
    }
    return pools;
}

function swissAutoMatchups(si) {
    const pools = swissGetActivePools(si);
    const matchups = [];
    for (const key of Object.keys(pools).sort()) {
        const pool = pools[key];
        const half = Math.floor(pool.length / 2);
        for (let i = 0; i < half; i++) {
            matchups.push({team1: pool[i], team2: pool[pool.length - 1 - i]});
        }
    }
    swissStages[si].nextMatchups = matchups;
    swissRender();
}

function swissAddMatchup(si) {
    const pools = swissGetActivePools(si);
    const allActive = Object.values(pools).flat();
    if (allActive.length < 2) return;
    if (!swissStages[si].nextMatchups) swissStages[si].nextMatchups = [];
    swissStages[si].nextMatchups.push({team1: allActive[0], team2: allActive[1]});
    swissRender();
}

function swissRemoveMatchup(si, idx) {
    swissStages[si].nextMatchups.splice(idx, 1);
    swissRender();
}

function swissMatchupChange(si, idx, side, val) {
    swissStages[si].nextMatchups[idx][side] = val;
    brkAutosave();
}

function swissAddAdvancers(si) {
    const sel = document.getElementById('swiss-adv-src-'+si);
    if (!sel) return;
    const fromStage = parseInt(sel.value);
    const srcSt = swissStages[fromStage];
    const advCount = Math.floor(srcSt.slots.length / 2);
    for (let i = 0; i < advCount; i++) {
        swissStages[si].slots.push({type:'advance', stage: fromStage});
    }
    swissRender();
}

function swissRender() {
    const area = document.getElementById('swiss-stages');
    let html = '';
    for (let si = 0; si < swissStages.length; si++) {
        const st = swissStages[si];
        html += '<div class="swiss-stage">';
        html += '<div class="swiss-stage-hdr">';
        html += '<span class="stage-label" onclick="swissRenameStage('+si+')" title="Click to rename">'+st.label+'</span>';
        html += '<div style="display:flex;gap:6px;">';
        if (swissStages.length > 1) html += '<button class="round-action danger" onclick="swissRemoveStage('+si+')" title="Remove stage">&times;</button>';
        html += '</div></div>';

        html += '<div class="swiss-cfg">';
        html += '<div><label>Teams</label><select onchange="swissSetTeamCount('+si+',parseInt(this.value))">';
        for (const c of [8,16,24,32]) {
            html += '<option value="'+c+'"'+(st.slots.length===c?' selected':'')+'>'+c+'</option>';
        }
        html += '</select></div>';
        html += '<div><label>Wins to Advance</label><input type="number" min="1" max="5" value="'+st.winsToAdv+'" onchange="swissStages['+si+'].winsToAdv=parseInt(this.value);brkAutosave()" style="width:50px;"></div>';
        html += '<div><label>Losses to Elim</label><input type="number" min="1" max="5" value="'+st.lossesToElim+'" onchange="swissStages['+si+'].lossesToElim=parseInt(this.value);brkAutosave()" style="width:50px;"></div>';
        const mBo = st.matchBo || 1;
        const dBo = st.deciderBo || 3;
        html += '<div><label>Match BO</label><select onchange="swissStages['+si+'].matchBo=parseInt(this.value);brkAutosave()">';
        html += '<option value="1"'+(mBo===1?' selected':'')+'>BO1</option>';
        html += '<option value="3"'+(mBo===3?' selected':'')+'>BO3</option>';
        html += '</select></div>';
        html += '<div><label>Decider BO</label><select onchange="swissStages['+si+'].deciderBo=parseInt(this.value);brkAutosave()">';
        html += '<option value="1"'+(dBo===1?' selected':'')+'>BO1</option>';
        html += '<option value="3"'+(dBo===3?' selected':'')+'>BO3</option>';
        html += '</select></div>';
        html += '<div style="display:flex;align-items:flex-end;"><label style="display:flex;align-items:center;gap:4px;cursor:pointer;"><input type="checkbox" '+(st.midStage?'checked':'')+' onchange="swissToggleMidStage('+si+')" style="accent-color:#4fc3f7;"> Mid-Stage</label></div>';
        html += '</div>';

        html += '<div class="swiss-team-grid">';
        for (let i = 0; i < st.slots.length; i++) {
            const slot = st.slots[i];
            html += '<div class="swiss-team-slot">';
            html += '<span class="seed-num">#'+(i+1)+'</span>';
            if (slot.type === 'advance') {
                const srcLabel = slot.stage < swissStages.length ? swissStages[slot.stage].label : '?';
                html += '<div class="adv-tag"><span>Adv. from '+srcLabel+'</span>';
                html += '<button class="remove-adv" onclick="swissSlotToTeam('+si+','+i+')" title="Change to team">&times;</button></div>';
            } else {
                html += '<select onchange="swissSlotChange('+si+','+i+',this.value)" style="'+(st.midStage?'flex:1;':'flex:1;')+'">';
                for (const t of BRK_ALL_TEAMS) {
                    html += '<option value="'+t+'"'+(slot.team===t?' selected':'')+'>'+t+'</option>';
                }
                if (si > 0) {
                    for (let ps = 0; ps < si; ps++) {
                        const aVal = 'ADV:'+ps;
                        html += '<option value="'+aVal+'" style="color:#81c784">-- Adv. from '+swissStages[ps].label+' --</option>';
                    }
                }
                html += '</select>';
                if (st.midStage) {
                    const maxW = st.winsToAdv - 1;
                    const maxL = st.lossesToElim - 1;
                    html += '<input type="number" min="0" max="'+st.winsToAdv+'" value="'+(slot.wins||0)+'" onchange="swissSetRecord('+si+','+i+',\\'wins\\',this.value)" title="Wins" style="width:36px;text-align:center;background:#1a2e1a;color:#81c784;border:1px solid #2e7d32;border-radius:4px;font-size:11px;padding:3px;">';
                    html += '<span style="color:#555;font-size:11px;">-</span>';
                    html += '<input type="number" min="0" max="'+st.lossesToElim+'" value="'+(slot.losses||0)+'" onchange="swissSetRecord('+si+','+i+',\\'losses\\',this.value)" title="Losses" style="width:36px;text-align:center;background:#2e1a1a;color:#ef5350;border:1px solid #7d2e2e;border-radius:4px;font-size:11px;padding:3px;">';
                }
            }
            html += '</div>';
        }
        html += '</div>';

        if (st.midStage) {
            const matchups = st.nextMatchups || [];
            const pools = swissGetActivePools(si);
            const allActive = Object.values(pools).flat();
            html += '<div style="margin-top:10px;padding:8px;background:#111;border:1px solid #333;border-radius:6px;">';
            html += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">';
            html += '<span style="color:#4fc3f7;font-size:12px;font-weight:bold;">Next Round Matchups</span>';
            html += '<div style="display:flex;gap:4px;">';
            html += '<button class="btn" onclick="swissAutoMatchups('+si+')" style="padding:2px 8px;font-size:10px;background:#1a1a2e;border-color:#4fc3f7;color:#4fc3f7;">Auto-pair by seed</button>';
            html += '<button class="btn" onclick="swissAddMatchup('+si+')" style="padding:2px 8px;font-size:10px;background:#1a2e1a;border-color:#2e7d32;color:#81c784;">+ Add</button>';
            html += '</div></div>';
            if (matchups.length === 0) {
                html += '<div style="color:#555;font-size:11px;padding:4px 0;">No matchups set — simulation will pair by seed. Click "Auto-pair" or add manually.</div>';
            }
            for (let mi = 0; mi < matchups.length; mi++) {
                const mu = matchups[mi];
                const rec1 = st.slots.find(s => s.type==='team' && s.team===mu.team1);
                const rec2 = st.slots.find(s => s.type==='team' && s.team===mu.team2);
                const tag1 = rec1 ? ' ('+(rec1.wins||0)+'-'+(rec1.losses||0)+')' : '';
                const tag2 = rec2 ? ' ('+(rec2.wins||0)+'-'+(rec2.losses||0)+')' : '';
                html += '<div style="display:flex;align-items:center;gap:6px;margin:3px 0;">';
                html += '<select onchange="swissMatchupChange('+si+','+mi+',\\'team1\\',this.value)" style="flex:1;padding:3px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;font-size:11px;">';
                for (const t of allActive) {
                    html += '<option value="'+t+'"'+(mu.team1===t?' selected':'')+'>'+t+'</option>';
                }
                html += '</select>';
                html += '<span style="color:#4fc3f7;font-size:11px;">vs</span>';
                html += '<select onchange="swissMatchupChange('+si+','+mi+',\\'team2\\',this.value)" style="flex:1;padding:3px;background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;font-size:11px;">';
                for (const t of allActive) {
                    html += '<option value="'+t+'"'+(mu.team2===t?' selected':'')+'>'+t+'</option>';
                }
                html += '</select>';
                html += '<button onclick="swissRemoveMatchup('+si+','+mi+')" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:14px;padding:0 4px;" title="Remove">&times;</button>';
                html += '</div>';
            }
            html += '</div>';
        }

        if (si > 0) {
            html += '<div class="swiss-add-adv">';
            html += '<select id="swiss-adv-src-'+si+'" style="background:#1a1a2e;color:#e0e0e0;border:1px solid #333;border-radius:4px;padding:3px 6px;">';
            for (let ps = 0; ps < si; ps++) {
                html += '<option value="'+ps+'">'+swissStages[ps].label+'</option>';
            }
            html += '</select>';
            html += '<button class="btn btn-compute" style="padding:3px 10px;font-size:11px;background:#1a2e1a;border-color:#2e7d32;" onclick="swissAddAdvancers('+si+')">+ Add Advancers</button>';
            html += '</div>';
        }

        html += '</div>';
    }
    area.innerHTML = html;
    brkAutosave();
}

async function swissSimulate() {
    if (swissStages.length === 0) { alert('Add at least one stage'); return; }
    const sims = parseInt(document.getElementById('bracket-sims').value) || 100000;

    const stages = swissStages.map(st => ({
        label: st.label,
        wins_to_advance: st.winsToAdv,
        losses_to_eliminate: st.lossesToElim,
        mid_stage: st.midStage || false,
        match_bo: st.matchBo || 1,
        decider_bo: st.deciderBo || 3,
        next_matchups: (st.nextMatchups || []).map(m => ({team1: m.team1, team2: m.team2})),
        slots: st.slots.map(s => s.type === 'advance' ? {type:'advance', stage:s.stage} : {type:'team', team:s.team, wins:s.wins||0, losses:s.losses||0}),
    }));

    document.getElementById('bracket-loading').style.display = 'block';
    document.getElementById('bracket-results').innerHTML = '';

    try {
        const resp = await fetch('/api/swiss_simulate', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({stages, sims}),
        });
        const data = await resp.json();
        swissRenderResults(data);
    } catch(e) {
        document.getElementById('bracket-results').innerHTML =
            '<div class="alert alert-error">Error: '+e.message+'</div>';
    }
    document.getElementById('bracket-loading').style.display = 'none';
}

function swissRenderResults(data) {
    if (data.error) {
        document.getElementById('bracket-results').innerHTML =
            '<div class="alert alert-error">'+data.error+'</div>';
        return;
    }
    let html = '<div style="margin-top:16px;">';
    html += '<p style="font-size:12px;color:#888;margin-bottom:8px;">'+data.sims.toLocaleString()+' simulations</p>';

    for (const stage of data.stages) {
        html += '<h2 style="color:#4fc3f7;margin-top:20px;">'+stage.label+'</h2>';

        html += '<table class="brk-results-table"><thead><tr>';
        html += '<th>Team</th><th style="text-align:right;">Advance %</th>';
        const advW = stage.wins_to_advance;
        const elimL = stage.losses_to_eliminate;
        const recordKeys = [];
        for (let w = advW; w >= 0; w--) {
            for (let l = 0; l <= elimL; l++) {
                if (w === advW || l === elimL) {
                    const k = w+'-'+l;
                    recordKeys.push(k);
                }
            }
        }
        for (const k of recordKeys) html += '<th style="text-align:right;font-size:11px;">'+k+'</th>';
        html += '</tr></thead><tbody>';

        const results = stage.results;
        const maxAdv = Math.max(...results.map(r => r.advance_pct));

        for (const r of results) {
            const advPct = (r.advance_pct * 100).toFixed(1);
            const isAdv = r.advance_pct >= 0.5;
            const cls = r.advance_pct >= 0.25 ? 'pct-high' : 'pct';
            html += '<tr><td class="team-name">'+r.team+'</td>';
            html += '<td style="text-align:right" class="'+cls+'">'+advPct+'%</td>';
            for (const k of recordKeys) {
                const val = r.records[k] || 0;
                const pct = (val * 100).toFixed(1);
                html += '<td style="text-align:right;font-size:12px;color:'+(parseFloat(pct)>10?'#e0e0e0':'#555')+'">'+pct+'%</td>';
            }
            html += '</tr>';
        }
        html += '</tbody></table>';

        html += '<h3 style="margin-top:16px;color:#e0e0e0;font-size:13px;">Advancement Odds</h3>';
        for (const r of results) {
            const v = r.advance_pct;
            if (v <= 0) continue;
            const pct = (v * 100).toFixed(1);
            const barW = (v / maxAdv * 100).toFixed(0);
            html += '<div style="display:flex;align-items:center;gap:10px;margin:4px 0;">';
            html += '<span style="min-width:170px;font-size:13px;color:#e0e0e0;">'+r.team+'</span>';
            html += '<div style="flex:1;max-width:300px;height:6px;background:#222;border-radius:3px;">';
            html += '<div class="swiss-record-fill adv" style="width:'+barW+'%;"></div></div>';
            html += '<span style="font-size:13px;color:#4fc3f7;font-weight:600;min-width:55px;">'+pct+'%</span>';
            html += '</div>';
        }
    }

    html += '</div>';
    document.getElementById('bracket-results').innerHTML = html;
}

// ── Save / Load Brackets ─────────────────────────────────────

function brkSave() {
    const name = prompt('Name this bracket:', brkActiveName || '');
    if (!name || !name.trim()) return;
    brkActiveName = name.trim();
    const saves = JSON.parse(localStorage.getItem('cs2_brackets') || '{}');
    saves[brkActiveName] = {
        swissStages: JSON.parse(JSON.stringify(swissStages)),
        brkRounds: JSON.parse(JSON.stringify(brkRounds)),
    };
    localStorage.setItem('cs2_brackets', JSON.stringify(saves));
    brkRefreshSaves();
}

function brkLoad(name) {
    if (!name) return;
    const saves = JSON.parse(localStorage.getItem('cs2_brackets') || '{}');
    if (!saves[name]) return;
    brkActiveName = name;
    const data = saves[name];
    swissStages = data.swissStages || [];
    brkRounds = data.brkRounds || [];
    swissMode = swissStages.length > 0;
    document.getElementById('swiss-panel').style.display = swissStages.length > 0 ? '' : 'none';
    document.getElementById('brk-bracket-area').style.display = brkRounds.length > 0 || swissStages.length === 0 ? '' : 'none';
    if (swissStages.length > 0 && brkRounds.length === 0) {
        document.getElementById('brk-bracket-area').style.display = 'none';
    }
    if (swissStages.length > 0) swissRender();
    brkRender();
    document.getElementById('bracket-results').innerHTML = '';
    document.getElementById('bracket-saves').value = '';
}

function brkDeleteSaved() {
    const sel = document.getElementById('bracket-saves');
    const name = sel.value;
    if (!name) { alert('Select a saved bracket to delete'); return; }
    if (!confirm('Delete "' + name + '"?')) return;
    const saves = JSON.parse(localStorage.getItem('cs2_brackets') || '{}');
    delete saves[name];
    localStorage.setItem('cs2_brackets', JSON.stringify(saves));
    brkRefreshSaves();
}

function brkRefreshSaves() {
    const saves = JSON.parse(localStorage.getItem('cs2_brackets') || '{}');
    const sel = document.getElementById('bracket-saves');
    if (!sel) return;
    const names = Object.keys(saves).sort();
    sel.innerHTML = '<option value="">-- Saved Brackets --</option>';
    for (const name of names) {
        sel.innerHTML += '<option value="' + name + '">' + name + '</option>';
    }
}

// ── Futures / Props Trading ──────────────────────────────────────
let futuresMarkets = [];
let lastBracketSimData = null;

async function futuresFetchEvent() {
    const url = document.getElementById('futures-url').value.trim();
    if (!url) { alert('Enter an event URL or ticker'); return; }
    document.getElementById('futures-loading').style.display = '';
    document.getElementById('futures-table-container').style.display = 'none';
    document.getElementById('futures-event-title').style.display = 'none';
    document.getElementById('futures-event-list').style.display = 'none';
    try {
        const resp = await fetch('/api/futures/fetch_event', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url}),
        });
        const data = await resp.json();
        if (data.error) { alert(data.error); return; }
        if (data.mode === 'event_list') {
            futuresRenderEventList(data.events);
        } else {
            futuresMarkets = data.markets;
            futuresMarkets.forEach(m => { m._checked = true; m._fair = null; });
            if (data.event_title) {
                const el = document.getElementById('futures-event-title');
                el.textContent = data.event_title;
                el.style.display = '';
            }
            futuresRenderTable();
        }
    } catch(e) {
        alert('Error: ' + e.message);
    }
    document.getElementById('futures-loading').style.display = 'none';
}

function futuresRenderEventList(events) {
    const el = document.getElementById('futures-event-list');
    let html = '<p style="color:#aaa;font-size:12px;margin-bottom:10px;">Select an event to load markets:</p>';
    for (const ev of events) {
        html += '<div class="futures-event-row" onclick="futuresLoadKalshiEvent(&quot;' + ev.event_ticker + '&quot;)" '
              + 'style="padding:12px 16px;margin-bottom:6px;background:#1a1a2e;border:1px solid #333;border-radius:6px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;">'
              + '<div><span style="color:#e0e0e0;font-weight:600;">'+ev.title+'</span>'
              + '<span style="color:#555;font-size:11px;margin-left:10px;">'+ev.event_ticker+'</span></div>'
              + '<span style="color:#4fc3f7;font-size:12px;">&rarr;</span></div>';
    }
    el.innerHTML = html;
    el.style.display = '';
}

async function futuresLoadKalshiEvent(eventTicker) {
    document.getElementById('futures-event-list').style.display = 'none';
    document.getElementById('futures-loading').style.display = '';
    try {
        const resp = await fetch('/api/futures/fetch_event', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url: 'KALSHI:' + eventTicker}),
        });
        const data = await resp.json();
        if (data.error) { alert(data.error); return; }
        futuresMarkets = data.markets;
        futuresMarkets.forEach(m => { m._checked = true; m._fair = null; });
        if (data.event_title) {
            const el = document.getElementById('futures-event-title');
            el.textContent = data.event_title;
            el.style.display = '';
        }
        futuresRenderTable();
    } catch(e) {
        alert('Error: ' + e.message);
    }
    document.getElementById('futures-loading').style.display = 'none';
}

function futuresRenderTable() {
    const tbody = document.getElementById('futures-tbody');
    const spread = parseInt(document.getElementById('futures-spread').value) || 5;
    let html = '';
    for (let i = 0; i < futuresMarkets.length; i++) {
        const m = futuresMarkets[i];
        const fairVal = m._fair;
        const askYesRaw = m.best_ask_yes != null ? m.best_ask_yes : m.best_ask;
        const askNoRaw = m.best_ask_no != null ? m.best_ask_no : null;
        const askYes = askYesRaw != null ? (askYesRaw * 100).toFixed(1) : '--';
        const askNo = askNoRaw != null ? (askNoRaw * 100).toFixed(1) : '--';
        let edgeYes = '--', edgeNo = '--', bidYes = '--', bidNo = '--';
        let edgeYesColor = '#888', edgeNoColor = '#888';
        if (fairVal != null) {
            if (askYesRaw != null) {
                const ey = fairVal - askYesRaw * 100;
                edgeYes = (ey >= 0 ? '+' : '') + ey.toFixed(1);
                edgeYesColor = ey > 0 ? '#81c784' : '#ef5350';
            }
            if (askNoRaw != null) {
                const en = (100 - fairVal) - askNoRaw * 100;
                edgeNo = (en >= 0 ? '+' : '') + en.toFixed(1);
                edgeNoColor = en > 0 ? '#81c784' : '#ef5350';
            }
            bidYes = Math.max(1, fairVal - spread).toFixed(0);
            bidNo = Math.max(1, (100 - fairVal) - spread).toFixed(0);
        }
        const checked = m._checked !== false ? 'checked' : '';
        html += '<tr>';
        html += '<td><input type="checkbox" class="futures-check" data-idx="'+i+'" '+checked+' onchange="futuresMarkets['+i+']._checked=this.checked"></td>';
        html += '<td class="team-name">'+m.team+'</td>';
        html += '<td style="text-align:right;">'+askYes+'c</td>';
        html += '<td style="text-align:right;">'+askNo+'c</td>';
        html += '<td style="text-align:right;"><input type="number" class="futures-fair" data-idx="'+i+'" '
              + 'value="'+(fairVal != null ? fairVal : '')+'" min="1" max="99" step="1" '
              + 'style="width:70px;text-align:right;background:#111;color:#4fc3f7;border:1px solid #333;border-radius:3px;padding:3px;" '
              + 'onchange="futuresUpdateFair('+i+', this.value)"></td>';
        html += '<td style="text-align:right;color:'+edgeYesColor+'">'+edgeYes+'c</td>';
        html += '<td style="text-align:right;color:'+edgeNoColor+'">'+edgeNo+'c</td>';
        html += '<td style="text-align:right;color:#4fc3f7;">'+bidYes+'c</td>';
        html += '<td style="text-align:right;color:#e0a0ff;">'+bidNo+'c</td>';
        html += '</tr>';
    }
    tbody.innerHTML = html;
    document.getElementById('futures-table-container').style.display = '';
}

function futuresUpdateFair(idx, val) {
    futuresMarkets[idx]._fair = val ? parseFloat(val) : null;
    futuresRenderTable();
}

function futuresToggleAll(el) {
    document.querySelectorAll('.futures-check').forEach(cb => { cb.checked = el.checked; });
    futuresMarkets.forEach(m => { m._checked = el.checked; });
}

function futuresImportBracket() {
    if (!lastBracketSimData) { alert('Run a bracket simulation first'); return; }
    if (futuresMarkets.length === 0) { alert('Fetch a futures event first'); return; }

    const options = [];
    if (lastBracketSimData.swiss_stages) {
        for (const stage of lastBracketSimData.swiss_stages) {
            options.push({label: stage.label + ' Advance %', type: 'swiss', stageLabel: stage.label});
        }
    }
    if (lastBracketSimData.labels) {
        for (const l of lastBracketSimData.labels) {
            options.push({label: l, type: 'round', roundLabel: l});
        }
    }
    if (options.length === 0) { alert('No probability columns found'); return; }

    let selectHtml = '<select id="futures-import-select" style="background:#111;color:#4fc3f7;border:1px solid #333;padding:6px 10px;border-radius:4px;font-size:14px;width:100%;">';
    for (let i = 0; i < options.length; i++) {
        selectHtml += '<option value="'+i+'">'+options[i].label+'</option>';
    }
    selectHtml += '</select>';

    const dialog = document.createElement('div');
    dialog.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);z-index:9999;display:flex;align-items:center;justify-content:center;';
    dialog.innerHTML = '<div style="background:#1a1a2e;border:1px solid #333;border-radius:8px;padding:24px;min-width:320px;">'
        + '<h3 style="color:#4fc3f7;margin:0 0 16px 0;">Import Probabilities</h3>'
        + '<label style="color:#aaa;font-size:13px;">Select column:</label>'
        + '<div style="margin:8px 0 20px 0;">'+selectHtml+'</div>'
        + '<div style="display:flex;gap:10px;justify-content:flex-end;">'
        + '<button id="futures-import-cancel" style="padding:8px 16px;background:#333;color:#ccc;border:none;border-radius:4px;cursor:pointer;">Cancel</button>'
        + '<button id="futures-import-confirm" style="padding:8px 16px;background:#1565c0;color:#fff;border:none;border-radius:4px;cursor:pointer;">Import</button>'
        + '</div></div>';
    document.body.appendChild(dialog);

    document.getElementById('futures-import-cancel').onclick = () => dialog.remove();
    document.getElementById('futures-import-confirm').onclick = () => {
        const sel = options[parseInt(document.getElementById('futures-import-select').value)];
        dialog.remove();
        _futuresApplyImport(sel);
    };
}

function _futuresApplyImport(sel) {
    const bracketProbs = {};

    if (sel.type === 'swiss') {
        for (const stage of lastBracketSimData.swiss_stages) {
            if (stage.label === sel.stageLabel) {
                for (const r of stage.results) {
                    if (r.advance_pct > 0) {
                        bracketProbs[r.team.toLowerCase()] = r.advance_pct;
                    }
                }
                break;
            }
        }
    } else if (sel.type === 'round') {
        for (const r of lastBracketSimData.results) {
            const val = r.rounds[sel.roundLabel];
            if (val !== 'bye' && val > 0) {
                bracketProbs[r.team.toLowerCase()] = val;
            }
        }
    }

    let matched = 0;
    for (const m of futuresMarkets) {
        const teamLower = m.team.toLowerCase();
        let prob = bracketProbs[teamLower];
        if (prob === undefined) {
            for (const [k, v] of Object.entries(bracketProbs)) {
                if (k.includes(teamLower) || teamLower.includes(k)) { prob = v; break; }
            }
        }
        if (prob !== undefined) {
            m._fair = Math.round(prob * 100);
            matched++;
        }
    }

    if (matched > 0) {
        futuresRenderTable();
        document.getElementById('futures-status').textContent =
            'Imported ' + matched + '/' + futuresMarkets.length + ' from "' + sel.label + '"';
    } else {
        alert('No team names matched between bracket results and futures markets');
    }
}

async function futuresPostOrders() {
    const spread = parseInt(document.getElementById('futures-spread').value) || 5;
    const size = parseInt(document.getElementById('futures-size').value) || 50;
    const orders = [];

    for (let i = 0; i < futuresMarkets.length; i++) {
        const m = futuresMarkets[i];
        if (!m._checked || m._fair == null) continue;
        const entry = {team: m.team, fair_prob: m._fair / 100, platform: m.platform};
        if (m.token_id) { entry.token_id = m.token_id; entry.tick_size = m.tick_size; entry.neg_risk = m.neg_risk; }
        if (m.token_yes) entry.token_yes = m.token_yes;
        if (m.token_no) entry.token_no = m.token_no;
        if (m.ticker) { entry.ticker = m.ticker; }
        orders.push(entry);
    }

    if (orders.length === 0) { alert('No orders to post (check boxes and set fair probs)'); return; }
    document.getElementById('futures-status').textContent = 'Posting ' + orders.length + ' orders...';

    try {
        const resp = await fetch('/api/futures/post_orders', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({orders, spread_cents: spread, size}),
        });
        const data = await resp.json();
        document.getElementById('futures-status').textContent = data.message || JSON.stringify(data);
    } catch(e) {
        document.getElementById('futures-status').textContent = 'Error: ' + e.message;
    }
}

async function futuresCancelAll() {
    document.getElementById('futures-status').textContent = 'Cancelling...';
    const tokenIds = [];
    futuresMarkets.filter(m => m.platform === 'poly').forEach(m => {
        if (m.token_yes) tokenIds.push(m.token_yes);
        if (m.token_no) tokenIds.push(m.token_no);
        if (!m.token_yes && m.token_id) tokenIds.push(m.token_id);
    });
    const tickers = futuresMarkets.filter(m => m.platform === 'kalshi').map(m => m.ticker);

    try {
        const resp = await fetch('/api/futures/cancel_all', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({token_ids: tokenIds, tickers: tickers}),
        });
        const data = await resp.json();
        document.getElementById('futures-status').textContent = data.message || 'Cancelled';
    } catch(e) {
        document.getElementById('futures-status').textContent = 'Error: ' + e.message;
    }
}

async function futuresRefreshAsks() {
    if (futuresMarkets.length === 0) return;
    document.getElementById('futures-status').textContent = 'Refreshing orderbooks...';
    try {
        const resp = await fetch('/api/futures/fetch_event', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url: document.getElementById('futures-url').value.trim()}),
        });
        const data = await resp.json();
        if (data.markets) {
            for (const fresh of data.markets) {
                const existing = futuresMarkets.find(m => m.team === fresh.team);
                if (existing) {
                    existing.best_ask = fresh.best_ask;
                    existing.best_ask_yes = fresh.best_ask_yes;
                    existing.best_ask_no = fresh.best_ask_no;
                    existing.market_price = fresh.market_price;
                }
            }
            futuresRenderTable();
        }
        document.getElementById('futures-status').textContent = 'Asks refreshed';
    } catch(e) {
        document.getElementById('futures-status').textContent = 'Error: ' + e.message;
    }
}

// ── Screen Tracker (Odds OCR) ─────────────────────────────
let stScreenData = null;
let stRegionMode = null;
let stSubMode = null;
let stScoreboard = null;
let stSubRegions = {};
let stDrawStart = null;
let stSbDrawStart = null;
let stTrackerPollInterval = null;
let stStreamRunning = false;
let stStreamRAF = null;
let stStreamFetching = false;
let stStreamFrameCount = 0;
let stStreamLastFpsTime = 0;
let stSbPreviewInterval = null;
let stTrackerRunning = false;
let stLastHome = null;
let stLastAway = null;

let stZoom = 1;
let stPanX = 0, stPanY = 0;
let stIsPanning = false;
let stPanLast = null;

const ST_REGION_COLORS = {home_odds: '#4caf50', away_odds: '#ef5350'};
const ST_REGION_LABELS = {home_odds: 'Home', away_odds: 'Away'};

function stUpdateTransform() {
    const inner = document.getElementById('st-screen-inner');
    if (inner) inner.style.transform = 'scale(' + stZoom + ') translate(' + stPanX + 'px,' + stPanY + 'px)';
    const el = document.getElementById('st-zoom-level');
    if (el) el.textContent = stZoom > 1 ? stZoom.toFixed(1) + 'x' : '';
}

function stZoomIn() { stZoom = Math.min(6, stZoom + 0.5); stUpdateTransform(); }
function stZoomOut() { stZoom = Math.max(1, stZoom - 0.5); if (stZoom <= 1) { stZoom = 1; stPanX = 0; stPanY = 0; } stUpdateTransform(); }
function stZoomReset() { stZoom = 1; stPanX = 0; stPanY = 0; stUpdateTransform(); }

// ── Live view streaming ──
async function stStreamFrame() {
    if (!stStreamRunning || stStreamFetching) return;
    stStreamFetching = true;
    try {
        const resp = await fetch('/api/st_screenshot', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({fast: true}),
        });
        const data = await resp.json();
        if (data.error) { stStreamFetching = false; return; }
        stScreenData = data;
        const img = document.getElementById('st-screen-img');
        img.src = 'data:image/' + (data.format || 'png') + ';base64,' + data.base64;
        img.onload = () => {
            img.style.display = 'block';
            document.getElementById('st-stream-placeholder').style.display = 'none';
            const canvas = document.getElementById('st-screen-canvas');
            if (canvas.width !== img.naturalWidth || canvas.height !== img.naturalHeight) {
                canvas.width = img.naturalWidth;
                canvas.height = img.naturalHeight;
            }
            stRedrawCanvas();
            stStreamFrameCount++;
            const now = performance.now();
            if (now - stStreamLastFpsTime > 2000) {
                const fps = (stStreamFrameCount / ((now - stStreamLastFpsTime) / 1000)).toFixed(1);
                document.getElementById('st-stream-fps').textContent = fps + ' fps';
                stStreamFrameCount = 0;
                stStreamLastFpsTime = now;
            }
        };
    } catch(e) {}
    stStreamFetching = false;
    if (stStreamRunning) {
        stStreamRAF = setTimeout(stStreamFrame, 0);
    }
}

function stToggleStream() {
    if (stStreamRunning) {
        stStreamRunning = false;
        if (stStreamRAF) { clearTimeout(stStreamRAF); stStreamRAF = null; }
        document.getElementById('st-stream-toggle').textContent = 'Start Live View';
        document.getElementById('st-stream-toggle').style.background = '#333';
        document.getElementById('st-stream-toggle').style.color = '';
        document.getElementById('st-stream-fps').textContent = '';
    } else {
        stStreamRunning = true;
        stStreamLastFpsTime = performance.now();
        stStreamFrameCount = 0;
        document.getElementById('st-stream-toggle').textContent = 'Stop Live View';
        document.getElementById('st-stream-toggle').style.background = '#66bb6a';
        document.getElementById('st-stream-toggle').style.color = '#000';
        stStreamFrame();
    }
}

// ── Step 1: Mark scoreboard on live view ──
function stSetRegionMode(mode) {
    if (!stStreamRunning) stToggleStream();
    stRegionMode = mode;
    document.querySelectorAll('.st-region-btn').forEach(b => {
        b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
    });
    const btn = document.getElementById('st-rgn-btn-' + mode);
    if (btn) {
        btn.style.background = '#4fc3f7'; btn.style.color = '#000'; btn.style.borderColor = '#4fc3f7';
    }
    const container = document.getElementById('st-screenshot-container');
    if (container) container.style.cursor = 'crosshair';
}

function stClearRegions() {
    stScoreboard = null;
    stSubRegions = {};
    stRegionMode = null;
    stSubMode = null;
    document.querySelectorAll('.st-region-btn').forEach(b => {
        b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
    });
    fetch('/api/st_clear_regions', {method: 'POST'});
    const canvas = document.getElementById('st-screen-canvas');
    if (canvas) { canvas.getContext('2d').clearRect(0, 0, canvas.width, canvas.height); }
    document.getElementById('st-sb-section').style.display = 'none';
    if (stSbPreviewInterval) { clearInterval(stSbPreviewInterval); stSbPreviewInterval = null; }
    document.getElementById('st-region-previews').innerHTML = '';
}

// ── Canvas drawing ──
function stRedrawCanvas() {
    const canvas = document.getElementById('st-screen-canvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (!stScreenData) return;
    const scaleX = canvas.width / stScreenData.width;
    const scaleY = canvas.height / stScreenData.height;
    if (stScoreboard) {
        ctx.strokeStyle = '#4fc3f7';
        ctx.lineWidth = 2;
        ctx.strokeRect(stScoreboard.x * scaleX, stScoreboard.y * scaleY,
                       stScoreboard.w * scaleX, stScoreboard.h * scaleY);
        ctx.fillStyle = '#4fc3f7';
        ctx.font = '12px monospace';
        ctx.fillText('Odds Region', stScoreboard.x * scaleX + 2, stScoreboard.y * scaleY - 4);
    }
    if (stDrawStart && stRegionMode) {
        ctx.strokeStyle = '#4fc3f7';
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 4]);
        ctx.strokeRect(stDrawStart.sx, stDrawStart.sy, stDrawStart.dx || 0, stDrawStart.dy || 0);
        ctx.setLineDash([]);
    }
}

function stGetCanvasCoords(e, canvasId) {
    const canvas = document.getElementById(canvasId || 'st-screen-canvas');
    const rect = canvas.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return {x: 0, y: 0};
    return {
        x: (e.clientX - rect.left) * (canvas.width / rect.width),
        y: (e.clientY - rect.top) * (canvas.height / rect.height),
    };
}

// Live view mouse handlers
(function() {
    const container = document.getElementById('st-screenshot-container');
    if (!container) return;

    container.addEventListener('wheel', function(e) {
        e.preventDefault();
        const delta = e.deltaY > 0 ? -0.3 : 0.3;
        stZoom = Math.max(1, Math.min(6, stZoom + delta));
        if (stZoom <= 1) { stZoom = 1; stPanX = 0; stPanY = 0; }
        stUpdateTransform();
    }, {passive: false});

    container.addEventListener('mousedown', function(e) {
        if (!stScreenData) return;
        e.preventDefault();
        if (stRegionMode === 'scoreboard') {
            const c = stGetCanvasCoords(e);
            stDrawStart = {sx: c.x, sy: c.y, dx: 0, dy: 0};
        } else {
            stIsPanning = true;
            stPanLast = {x: e.clientX, y: e.clientY};
            container.style.cursor = 'grabbing';
        }
    });

    container.addEventListener('mousemove', function(e) {
        if (stDrawStart) {
            const c = stGetCanvasCoords(e);
            stDrawStart.dx = c.x - stDrawStart.sx;
            stDrawStart.dy = c.y - stDrawStart.sy;
            stRedrawCanvas();
        } else if (stIsPanning && stPanLast) {
            const dx = (e.clientX - stPanLast.x) / stZoom;
            const dy = (e.clientY - stPanLast.y) / stZoom;
            stPanX += dx;
            stPanY += dy;
            stPanLast = {x: e.clientX, y: e.clientY};
            stUpdateTransform();
        }
    });

    container.addEventListener('mouseup', function(e) {
        if (stIsPanning) {
            stIsPanning = false;
            stPanLast = null;
            container.style.cursor = stRegionMode ? 'crosshair' : (stZoom > 1 ? 'grab' : 'default');
            return;
        }
        if (!stDrawStart || stRegionMode !== 'scoreboard' || !stScreenData) return;
        const canvas = document.getElementById('st-screen-canvas');
        const c = stGetCanvasCoords(e);
        let x1 = stDrawStart.sx, y1 = stDrawStart.sy;
        let x2 = c.x, y2 = c.y;
        if (Math.abs(x2 - x1) < 10 || Math.abs(y2 - y1) < 10) { stDrawStart = null; return; }
        const scaleX = stScreenData.width / canvas.width;
        const scaleY = stScreenData.height / canvas.height;
        stScoreboard = {
            x: Math.round(Math.min(x1, x2) * scaleX),
            y: Math.round(Math.min(y1, y2) * scaleY),
            w: Math.round(Math.abs(x2 - x1) * scaleX),
            h: Math.round(Math.abs(y2 - y1) * scaleY),
        };
        stDrawStart = null;
        stRegionMode = null;
        const btn = document.getElementById('st-rgn-btn-scoreboard');
        if (btn) { btn.style.background = '#1b3a2e'; btn.style.color = '#66bb6a'; btn.style.borderColor = '#66bb6a'; }
        container.style.cursor = stZoom > 1 ? 'grab' : 'default';
        stRedrawCanvas();

        fetch('/api/st_set_scoreboard', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(stScoreboard),
        });

        stShowSbPreview();
    });

    container.addEventListener('mouseleave', function() {
        if (stIsPanning) {
            stIsPanning = false;
            stPanLast = null;
            container.style.cursor = stRegionMode ? 'crosshair' : (stZoom > 1 ? 'grab' : 'default');
        }
    });
})();

// ── Step 2: Scoreboard preview + sub-region tagging ──
function stShowSbPreview() {
    document.getElementById('st-sb-section').style.display = 'block';
    stRefreshSbPreview();
    if (stSbPreviewInterval) clearInterval(stSbPreviewInterval);
    stSbPreviewInterval = setInterval(stRefreshSbPreview, 150);
}

async function stRefreshSbPreview() {
    try {
        const resp = await fetch('/api/st_scoreboard_capture', {method: 'POST'});
        const data = await resp.json();
        if (data.error) return;
        const img = document.getElementById('st-sb-img');
        img.src = 'data:image/jpeg;base64,' + data.base64;
        img.onload = () => {
            const canvas = document.getElementById('st-sb-canvas');
            if (canvas.width !== img.naturalWidth || canvas.height !== img.naturalHeight) {
                canvas.width = img.naturalWidth;
                canvas.height = img.naturalHeight;
            }
            stRedrawSbCanvas();
        };
    } catch(e) {}
}

function stSetSubMode(mode) {
    stSubMode = mode;
    document.querySelectorAll('.st-sub-btn').forEach(b => {
        b.style.background = '#333'; b.style.borderColor = '#555'; b.style.color = '#e0e0e0';
    });
    const btn = document.getElementById('st-sub-btn-' + mode);
    if (btn) {
        const color = ST_REGION_COLORS[mode] || '#fff';
        btn.style.background = color; btn.style.color = '#000'; btn.style.borderColor = color;
    }
    const preview = document.getElementById('st-sb-preview');
    if (preview) preview.style.cursor = 'crosshair';
}

function stRedrawSbCanvas() {
    const canvas = document.getElementById('st-sb-canvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const img = document.getElementById('st-sb-img');
    if (!img || !img.naturalWidth || !stScoreboard) return;
    for (const [key, r] of Object.entries(stSubRegions)) {
        const scaleX = canvas.width / stScoreboard.w;
        const scaleY = canvas.height / stScoreboard.h;
        const color = ST_REGION_COLORS[key] || '#fff';
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;
        ctx.strokeRect(r.x * scaleX, r.y * scaleY, r.w * scaleX, r.h * scaleY);
        ctx.fillStyle = color;
        ctx.font = '12px monospace';
        ctx.fillText(ST_REGION_LABELS[key] || key, r.x * scaleX + 2, r.y * scaleY - 4);
    }
    if (stSbDrawStart && stSubMode) {
        ctx.strokeStyle = ST_REGION_COLORS[stSubMode] || '#fff';
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 4]);
        ctx.strokeRect(stSbDrawStart.sx, stSbDrawStart.sy, stSbDrawStart.dx || 0, stSbDrawStart.dy || 0);
        ctx.setLineDash([]);
    }
}

// Scoreboard preview mouse handlers
(function() {
    const preview = document.getElementById('st-sb-preview');
    if (!preview) return;

    preview.addEventListener('mousedown', function(e) {
        if (!stSubMode) return;
        e.preventDefault();
        const c = stGetCanvasCoords(e, 'st-sb-canvas');
        stSbDrawStart = {sx: c.x, sy: c.y, dx: 0, dy: 0};
    });

    preview.addEventListener('mousemove', function(e) {
        if (!stSbDrawStart) return;
        const c = stGetCanvasCoords(e, 'st-sb-canvas');
        stSbDrawStart.dx = c.x - stSbDrawStart.sx;
        stSbDrawStart.dy = c.y - stSbDrawStart.sy;
        stRedrawSbCanvas();
    });

    preview.addEventListener('mouseup', function(e) {
        if (!stSbDrawStart || !stSubMode || !stScoreboard) return;
        const canvas = document.getElementById('st-sb-canvas');
        const c = stGetCanvasCoords(e, 'st-sb-canvas');
        let x1 = stSbDrawStart.sx, y1 = stSbDrawStart.sy;
        let x2 = c.x, y2 = c.y;
        if (Math.abs(x2 - x1) < 3 || Math.abs(y2 - y1) < 3) { stSbDrawStart = null; return; }

        const scaleX = stScoreboard.w / canvas.width;
        const scaleY = stScoreboard.h / canvas.height;
        const region = {
            x: Math.round(Math.min(x1, x2) * scaleX),
            y: Math.round(Math.min(y1, y2) * scaleY),
            w: Math.round(Math.abs(x2 - x1) * scaleX),
            h: Math.round(Math.abs(y2 - y1) * scaleY),
        };
        stSubRegions[stSubMode] = region;
        stSbDrawStart = null;
        stRedrawSbCanvas();

        fetch('/api/st_set_sub_region', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({name: stSubMode, region: region}),
        });

        const order = ['home_odds', 'away_odds'];
        const nextIdx = order.indexOf(stSubMode) + 1;
        if (nextIdx < order.length && !stSubRegions[order[nextIdx]]) {
            stSetSubMode(order[nextIdx]);
        } else {
            stSubMode = null;
            document.querySelectorAll('.st-sub-btn').forEach(b => {
                b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
            });
            preview.style.cursor = 'crosshair';
        }
        stUpdateRegionPreviews();
    });
})();

function stUpdateRegionPreviews() {
    const previews = document.getElementById('st-region-previews');
    let html = '';
    for (const key of ['home_odds', 'away_odds']) {
        const color = ST_REGION_COLORS[key];
        const label = ST_REGION_LABELS[key];
        const r = stSubRegions[key];
        if (r) {
            html += '<span style="font-size:11px;color:' + color + ';background:#1a1a2e;padding:2px 6px;border-radius:3px;">' +
                label + ': ' + r.x + ',' + r.y + ' ' + r.w + 'x' + r.h + '</span> ';
        }
    }
    previews.innerHTML = html;
}

async function stTestOCR() {
    const previews = document.getElementById('st-region-previews');
    previews.innerHTML = '<span style="color:#666;font-size:12px;">Testing OCR...</span>';
    try {
        const resp = await fetch('/api/st_capture_once', {method: 'POST'});
        const data = await resp.json();
        const odds = data.odds || {};
        let html = '';
        for (const key of ['home_odds', 'away_odds']) {
            const color = ST_REGION_COLORS[key];
            const label = ST_REGION_LABELS[key];
            const val = odds[key] != null ? odds[key] : '?';
            html += '<div style="text-align:center;margin-right:12px;">';
            html += '<div style="font-size:11px;color:' + color + ';font-weight:600;">' + label + '</div>';
            html += '<div style="font-size:16px;font-weight:700;color:#fff;">' + val + '</div>';
            html += '</div>';
        }
        if (data.confidence) {
            const confParts = Object.entries(data.confidence).map(([k,v]) => k + ':' + v);
            html += '<div style="font-size:11px;color:#666;margin-top:4px;">' + confParts.join('  ') + '</div>';
        }
        previews.innerHTML = html;
    } catch(e) {
        previews.innerHTML = '<span style="color:#ef5350;font-size:12px;">Error: ' + e.message + '</span>';
    }
}

function stToggleTracker() {
    if (stTrackerRunning) {
        fetch('/api/st_stop', {method: 'POST'});
        clearInterval(stTrackerPollInterval);
        stTrackerRunning = false;
        document.getElementById('st-tracker-toggle').textContent = 'Start Tracking';
        document.getElementById('st-tracker-status').textContent = 'Stopped';
        document.getElementById('st-tracker-status').style.color = '#666';
        document.getElementById('st-tracker-live').style.display = 'none';
    } else {
        fetch('/api/st_start', {method: 'POST'});
        stTrackerRunning = true;
        document.getElementById('st-tracker-toggle').textContent = 'Stop Tracking';
        document.getElementById('st-tracker-status').textContent = 'Running';
        document.getElementById('st-tracker-status').style.color = '#4caf50';
        document.getElementById('st-tracker-live').style.display = '';
        stTrackerPollInterval = setInterval(stPollState, 200);
    }
}

async function stPollState() {
    try {
        const resp = await fetch('/api/st_state');
        const s = await resp.json();
        const odds = s.odds || {};
        const ho = odds.home_odds != null ? odds.home_odds : null;
        const ao = odds.away_odds != null ? odds.away_odds : null;
        const homeEl = document.getElementById('st-ocr-home');
        const awayEl = document.getElementById('st-ocr-away');
        homeEl.textContent = ho !== null ? ho.toFixed(2) : '—';
        awayEl.textContent = ao !== null ? ao.toFixed(2) : '—';

        if (s.confidence) {
            const confParts = Object.entries(s.confidence).map(([k,v]) => k + ':' + v);
            document.getElementById('st-ocr-conf').textContent = confParts.join('  ');
        }

        if (stLastHome !== null && ho !== null) {
            const diff = Math.abs(ho - stLastHome);
            homeEl.style.color = diff >= 0.20 ? '#ff5722' : '#4caf50';
        }
        if (stLastAway !== null && ao !== null) {
            const diff = Math.abs(ao - stLastAway);
            awayEl.style.color = diff >= 0.20 ? '#ff5722' : '#ef5350';
        }
        if (ho !== null) stLastHome = ho;
        if (ao !== null) stLastAway = ao;

        const histResp = await fetch('/api/st_history');
        const hist = await histResp.json();
        const log = document.getElementById('st-history-log');
        let html = '';
        for (let i = hist.length - 1; i >= Math.max(0, hist.length - 50); i--) {
            const h = hist[i];
            const ts = new Date(h.ts * 1000).toLocaleTimeString();
            const ho2 = (h.odds || {}).home_odds;
            const ao2 = (h.odds || {}).away_odds;
            const hv = ho2 != null ? ho2.toFixed(2) : '—';
            const av = ao2 != null ? ao2.toFixed(2) : '—';
            html += '<div>' + ts + '  Home: <span style="color:#4caf50;">' + hv +
                    '</span>  Away: <span style="color:#ef5350;">' + av + '</span></div>';
        }
        log.innerHTML = html;
    } catch(e) {}
}

function stReset() {
    fetch('/api/st_reset', {method: 'POST'});
    stLastHome = null;
    stLastAway = null;
    document.getElementById('st-ocr-home').textContent = '—';
    document.getElementById('st-ocr-away').textContent = '—';
    document.getElementById('st-history-log').innerHTML = '';
}

// ── Screen Trade (in Trade tab) ─────────────────────────────
let sctScreenData = null;
let sctStreamRunning = false;
let sctStreamRAF = null;
let sctStreamFetching = false;
let sctStreamFrameCount = 0;
let sctStreamLastFpsTime = 0;
let sctScoreboard = null;
let sctSubRegions = {};
let sctRegionMode = null;
let sctSubMode = null;
let sctDrawStart = null;
let sctSbDrawStart = null;
let sctSbPreviewInterval = null;
let sctZoom = 1;
let sctPanX = 0, sctPanY = 0;
let sctIsPanning = false;
let sctPanLast = null;

let screenTrading = false;
let screenTradePoll = null;
let screenTradeMode = 'ioc';

let sctTrackedGames = {};

function sctGameKey(g) { return g.home + '|' + g.away; }

function sctUpdateTransform() {
    const inner = document.getElementById('sct-screen-inner');
    if (inner) inner.style.transform = 'scale(' + sctZoom + ') translate(' + sctPanX + 'px,' + sctPanY + 'px)';
}

function sctGetCanvasCoords(e, canvasId) {
    const canvas = document.getElementById(canvasId || 'sct-screen-canvas');
    const rect = canvas.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return {x: 0, y: 0};
    return {
        x: (e.clientX - rect.left) * (canvas.width / rect.width),
        y: (e.clientY - rect.top) * (canvas.height / rect.height),
    };
}

async function sctStreamFrame() {
    if (!sctStreamRunning || sctStreamFetching) return;
    sctStreamFetching = true;
    try {
        const resp = await fetch('/api/st_screenshot', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({fast: true}),
        });
        const data = await resp.json();
        if (!data.error) {
            sctScreenData = data;
            const img = document.getElementById('sct-screen-img');
            img.src = 'data:image/' + (data.format || 'png') + ';base64,' + data.base64;
            img.onload = () => {
                img.style.display = 'block';
                const canvas = document.getElementById('sct-screen-canvas');
                if (canvas.width !== img.naturalWidth || canvas.height !== img.naturalHeight) {
                    canvas.width = img.naturalWidth; canvas.height = img.naturalHeight;
                }
                sctRedrawCanvas();
                sctStreamFrameCount++;
                const now = performance.now();
                if (now - sctStreamLastFpsTime > 2000) {
                    const fps = (sctStreamFrameCount / ((now - sctStreamLastFpsTime) / 1000)).toFixed(1);
                    document.getElementById('sct-stream-fps').textContent = fps + ' fps';
                    sctStreamFrameCount = 0; sctStreamLastFpsTime = now;
                }
            };
        }
    } catch(e) {}
    sctStreamFetching = false;
    if (sctStreamRunning) sctStreamRAF = setTimeout(sctStreamFrame, 0);
}

function sctToggleStream() {
    const container = document.getElementById('sct-screenshot-container');
    if (sctStreamRunning) {
        sctStreamRunning = false;
        if (sctStreamRAF) { clearTimeout(sctStreamRAF); sctStreamRAF = null; }
        container.style.display = 'none';
        document.getElementById('sct-stream-toggle').textContent = 'Live View';
        document.getElementById('sct-stream-fps').textContent = '';
    } else {
        sctStreamRunning = true;
        sctStreamLastFpsTime = performance.now(); sctStreamFrameCount = 0;
        container.style.display = '';
        document.getElementById('sct-stream-toggle').textContent = 'Hide View';
        sctStreamFrame();
    }
}

function sctSetRegionMode(mode) {
    if (!sctStreamRunning) sctToggleStream();
    sctRegionMode = mode;
    document.querySelectorAll('.sct-region-btn').forEach(b => {
        b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
    });
    const btn = document.getElementById('sct-rgn-btn-' + mode);
    if (btn) { btn.style.background = '#7986cb'; btn.style.color = '#000'; btn.style.borderColor = '#7986cb'; }
    document.getElementById('sct-screenshot-container').style.cursor = 'crosshair';
}

function sctRedrawCanvas() {
    const canvas = document.getElementById('sct-screen-canvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (!sctScreenData) return;
    const sx = canvas.width / sctScreenData.width;
    const sy = canvas.height / sctScreenData.height;
    if (sctScoreboard) {
        ctx.strokeStyle = '#7986cb'; ctx.lineWidth = 2;
        ctx.strokeRect(sctScoreboard.x * sx, sctScoreboard.y * sy, sctScoreboard.w * sx, sctScoreboard.h * sy);
        ctx.fillStyle = '#7986cb'; ctx.font = '12px monospace';
        ctx.fillText('Odds Region', sctScoreboard.x * sx + 2, sctScoreboard.y * sy - 4);
    }
    if (sctDrawStart && sctRegionMode) {
        ctx.strokeStyle = '#7986cb'; ctx.lineWidth = 2; ctx.setLineDash([4, 4]);
        ctx.strokeRect(sctDrawStart.sx, sctDrawStart.sy, sctDrawStart.dx || 0, sctDrawStart.dy || 0);
        ctx.setLineDash([]);
    }
}

function sctClearAll() {
    sctScoreboard = null; sctSubRegions = {};
    sctRegionMode = null; sctSubMode = null;
    sctTrackedGames = {};
    fetch('/api/st_clear_regions', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({}),
    });
    const canvas = document.getElementById('sct-screen-canvas');
    if (canvas) canvas.getContext('2d').clearRect(0, 0, canvas.width, canvas.height);
    document.getElementById('sct-sb-section').style.display = 'none';
    if (sctSbPreviewInterval) { clearInterval(sctSbPreviewInterval); sctSbPreviewInterval = null; }
    sctRenderTrackedGames();
    sctRenderMarkButtons();
}

(function() {
    const container = document.getElementById('sct-screenshot-container');
    if (!container) return;
    container.addEventListener('wheel', function(e) {
        e.preventDefault();
        sctZoom = Math.max(1, Math.min(6, sctZoom + (e.deltaY > 0 ? -0.3 : 0.3)));
        if (sctZoom <= 1) { sctZoom = 1; sctPanX = 0; sctPanY = 0; }
        sctUpdateTransform();
    }, {passive: false});
    container.addEventListener('mousedown', function(e) {
        if (!sctScreenData) return;
        e.preventDefault();
        if (sctRegionMode === 'scoreboard') {
            sctDrawStart = sctGetCanvasCoords(e);
            sctDrawStart = {sx: sctDrawStart.x, sy: sctDrawStart.y, dx: 0, dy: 0};
        } else {
            sctIsPanning = true; sctPanLast = {x: e.clientX, y: e.clientY};
            container.style.cursor = 'grabbing';
        }
    });
    container.addEventListener('mousemove', function(e) {
        if (sctDrawStart) {
            const c = sctGetCanvasCoords(e);
            sctDrawStart.dx = c.x - sctDrawStart.sx; sctDrawStart.dy = c.y - sctDrawStart.sy;
            sctRedrawCanvas();
        } else if (sctIsPanning && sctPanLast) {
            sctPanX += (e.clientX - sctPanLast.x) / sctZoom;
            sctPanY += (e.clientY - sctPanLast.y) / sctZoom;
            sctPanLast = {x: e.clientX, y: e.clientY}; sctUpdateTransform();
        }
    });
    container.addEventListener('mouseup', function(e) {
        if (sctIsPanning) { sctIsPanning = false; sctPanLast = null;
            container.style.cursor = sctRegionMode ? 'crosshair' : 'default'; return; }
        if (!sctDrawStart || sctRegionMode !== 'scoreboard' || !sctScreenData) return;
        const canvas = document.getElementById('sct-screen-canvas');
        const c = sctGetCanvasCoords(e);
        let x1 = sctDrawStart.sx, y1 = sctDrawStart.sy, x2 = c.x, y2 = c.y;
        if (Math.abs(x2-x1) < 10 || Math.abs(y2-y1) < 10) { sctDrawStart = null; return; }
        const scX = sctScreenData.width / canvas.width, scY = sctScreenData.height / canvas.height;
        sctScoreboard = {
            x: Math.round(Math.min(x1,x2)*scX), y: Math.round(Math.min(y1,y2)*scY),
            w: Math.round(Math.abs(x2-x1)*scX), h: Math.round(Math.abs(y2-y1)*scY),
        };
        sctDrawStart = null; sctRegionMode = null;
        document.querySelectorAll('.sct-region-btn').forEach(b => {
            b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
        });
        container.style.cursor = 'default';
        sctRedrawCanvas();
        fetch('/api/st_set_scoreboard', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(sctScoreboard),
        });
        sctShowSbPreview();
    });
    container.addEventListener('mouseleave', function() {
        if (sctIsPanning) { sctIsPanning = false; sctPanLast = null; }
    });
})();

function sctShowSbPreview() {
    document.getElementById('sct-sb-section').style.display = '';
    sctRefreshSbPreview();
    if (sctSbPreviewInterval) clearInterval(sctSbPreviewInterval);
    sctSbPreviewInterval = setInterval(sctRefreshSbPreview, 150);
}

async function sctRefreshSbPreview() {
    try {
        const resp = await fetch('/api/st_scoreboard_capture', {method: 'POST'});
        const data = await resp.json();
        if (data.error) return;
        const img = document.getElementById('sct-sb-img');
        img.src = 'data:image/jpeg;base64,' + data.base64;
        img.onload = () => {
            const canvas = document.getElementById('sct-sb-canvas');
            if (canvas.width !== img.naturalWidth || canvas.height !== img.naturalHeight) {
                canvas.width = img.naturalWidth; canvas.height = img.naturalHeight;
            }
            sctRedrawSbCanvas();
        };
    } catch(e) {}
}

function sctSetSubMode(mode) {
    sctSubMode = mode;
    sctRenderMarkButtons();
    document.getElementById('sct-sb-preview').style.cursor = 'crosshair';
}

function sctRedrawSbCanvas() {
    const canvas = document.getElementById('sct-sb-canvas');
    if (!canvas || !sctScoreboard) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const sx = canvas.width / sctScoreboard.w, sy = canvas.height / sctScoreboard.h;
    for (const [key, r] of Object.entries(sctSubRegions)) {
        const isLabel = key.endsWith('_label');
        const isHome = key.includes('_home');
        const color = isLabel ? '#ffa726' : (isHome ? '#4caf50' : '#ef5350');
        const gameKey = key.replace(/_(?:home|away)(?:_label)?$/, '');
        const g = sctTrackedGames[gameKey];
        const label = isLabel ? 'Label' : (isHome ? (g ? g.game.home : 'Home') : (g ? g.game.away : 'Away'));
        ctx.strokeStyle = color; ctx.lineWidth = 2;
        ctx.strokeRect(r.x*sx, r.y*sy, r.w*sx, r.h*sy);
        ctx.fillStyle = color; ctx.font = '11px monospace';
        ctx.fillText(label, r.x*sx+2, r.y*sy-3);
    }
    if (sctSbDrawStart && sctSubMode) {
        const isLabel = sctSubMode.endsWith('_label');
        const isHome = sctSubMode.includes('_home');
        ctx.strokeStyle = isLabel ? '#ffa726' : (isHome ? '#4caf50' : '#ef5350'); ctx.lineWidth = 2;
        ctx.setLineDash([4,4]);
        ctx.strokeRect(sctSbDrawStart.sx, sctSbDrawStart.sy, sctSbDrawStart.dx||0, sctSbDrawStart.dy||0);
        ctx.setLineDash([]);
    }
}

(function() {
    const preview = document.getElementById('sct-sb-preview');
    if (!preview) return;
    preview.addEventListener('mousedown', function(e) {
        if (!sctSubMode) return; e.preventDefault();
        const c = sctGetCanvasCoords(e, 'sct-sb-canvas');
        sctSbDrawStart = {sx: c.x, sy: c.y, dx: 0, dy: 0};
    });
    preview.addEventListener('mousemove', function(e) {
        if (!sctSbDrawStart) return;
        const c = sctGetCanvasCoords(e, 'sct-sb-canvas');
        sctSbDrawStart.dx = c.x - sctSbDrawStart.sx; sctSbDrawStart.dy = c.y - sctSbDrawStart.sy;
        sctRedrawSbCanvas();
    });
    preview.addEventListener('mouseup', function(e) {
        if (!sctSbDrawStart || !sctSubMode || !sctScoreboard) return;
        const canvas = document.getElementById('sct-sb-canvas');
        const c = sctGetCanvasCoords(e, 'sct-sb-canvas');
        let x1 = sctSbDrawStart.sx, y1 = sctSbDrawStart.sy, x2 = c.x, y2 = c.y;
        if (Math.abs(x2-x1) < 3 || Math.abs(y2-y1) < 3) { sctSbDrawStart = null; return; }
        const scX = sctScoreboard.w / canvas.width, scY = sctScoreboard.h / canvas.height;
        const region = {
            x: Math.round(Math.min(x1,x2)*scX), y: Math.round(Math.min(y1,y2)*scY),
            w: Math.round(Math.abs(x2-x1)*scX), h: Math.round(Math.abs(y2-y1)*scY),
        };
        sctSubRegions[sctSubMode] = region;
        sctSbDrawStart = null;
        fetch('/api/st_set_sub_region', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({name: sctSubMode, region: region}),
        });
        if (sctSubMode.endsWith('_home')) {
            const awayKey = sctSubMode.replace(/_home$/, '_away');
            if (!sctSubRegions[awayKey]) {
                sctSubMode = awayKey;
                sctRedrawSbCanvas();
                sctRenderMarkButtons();
                return;
            }
        }
        sctSubMode = null;
        sctRedrawSbCanvas();
        sctRenderMarkButtons();
    });
})();

function sctRenderMarkButtons() {
    const el = document.getElementById('sct-mark-buttons');
    if (!el) return;
    const keys = Object.keys(sctTrackedGames);
    if (keys.length === 0) { el.innerHTML = ''; return; }
    let html = '';
    for (const key of keys) {
        const g = sctTrackedGames[key];
        const homeKey = key + '_home';
        const awayKey = key + '_away';
        const homeLabelKey = key + '_home_label';
        const awayLabelKey = key + '_away_label';
        const homeSet = !!sctSubRegions[homeKey];
        const awaySet = !!sctSubRegions[awayKey];
        const homeLabelSet = !!sctSubRegions[homeLabelKey];
        const awayLabelSet = !!sctSubRegions[awayLabelKey];
        const homeActive = sctSubMode === homeKey;
        const awayActive = sctSubMode === awayKey;
        const homeLabelActive = sctSubMode === homeLabelKey;
        const awayLabelActive = sctSubMode === awayLabelKey;
        html += '<span style="color:#777;font-size:10px;margin-right:2px;">' + g.game.home + ' v ' + g.game.away + ':</span>';
        html += '<button class="sct-sub-btn" data-subkey="' + homeKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (homeActive ? '#4caf50' : (homeSet ? '#1b5e20' : '#333'))
            + ';border:1px solid #4caf50;color:' + (homeActive ? '#000' : '#4caf50') + ';border-radius:3px;cursor:pointer;">'
            + g.game.home + (homeSet ? ' &#10003;' : '') + '</button>';
        html += '<button class="sct-sub-btn" data-subkey="' + homeLabelKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (homeLabelActive ? '#ffa726' : (homeLabelSet ? '#e65100' : '#333'))
            + ';border:1px solid #ffa726;color:' + (homeLabelActive ? '#000' : '#ffa726') + ';border-radius:3px;cursor:pointer;">'
            + 'Lbl' + (homeLabelSet ? ' &#10003;' : '') + '</button>';
        html += '<button class="sct-sub-btn" data-subkey="' + awayKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (awayActive ? '#ef5350' : (awaySet ? '#b71c1c' : '#333'))
            + ';border:1px solid #ef5350;color:' + (awayActive ? '#000' : '#ef5350') + ';border-radius:3px;cursor:pointer;">'
            + g.game.away + (awaySet ? ' &#10003;' : '') + '</button>';
        html += '<button class="sct-sub-btn" data-subkey="' + awayLabelKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (awayLabelActive ? '#ffa726' : (awayLabelSet ? '#e65100' : '#333'))
            + ';border:1px solid #ffa726;color:' + (awayLabelActive ? '#000' : '#ffa726') + ';border-radius:3px;cursor:pointer;">'
            + 'Lbl' + (awayLabelSet ? ' &#10003;' : '') + '</button>';
        html += '<span style="margin-right:10px;"></span>';
    }
    html += '<button onclick="sctTestAllOCR()" style="padding:2px 6px;font-size:10px;background:#555;border:1px solid #666;color:#ccc;border-radius:3px;cursor:pointer;">Test OCR</button>';
    el.innerHTML = html;
}

(function() {
    document.getElementById('sct-mark-buttons').addEventListener('click', function(e) {
        const btn = e.target.closest('[data-subkey]');
        if (btn) sctSetSubMode(btn.dataset.subkey);
    });
    document.getElementById('sct-tracked-games').addEventListener('click', function(e) {
        const btn = e.target.closest('[data-remove-key]');
        if (btn) { e.stopPropagation(); sctRemoveGame(btn.dataset.removeKey); }
    });
})();

async function sctTestAllOCR() {
    try {
        const resp = await fetch('/api/st_capture_once', {method: 'POST'});
        const data = await resp.json();
        const odds = data.odds || {};
        for (const [key, g] of Object.entries(sctTrackedGames)) {
            const safeKey = key.replace(/\\|/g, '_');
            const ho = odds[key + '_home'], ao = odds[key + '_away'];
            const rawH = document.getElementById('sct-tg-raw-' + safeKey + '-h');
            const rawA = document.getElementById('sct-tg-raw-' + safeKey + '-a');
            if (rawH) rawH.textContent = ho != null ? ho.toFixed(2) : 'fail';
            if (rawA) rawA.textContent = ao != null ? ao.toFixed(2) : 'fail';
            if (ho != null && ao != null) {
                const fair = powerDevig(ho, ao);
                const boost = g.boost || 0;
                const bhf = Math.max(1, Math.min(99, Math.round(fair.home * 100) + boost));
                const baf = 100 - bhf;
                const fairH = document.getElementById('sct-tg-fair-' + safeKey + '-h');
                const fairA = document.getElementById('sct-tg-fair-' + safeKey + '-a');
                if (fairH) fairH.textContent = bhf + 'c' + (boost ? ' (' + (boost > 0 ? '+' : '') + boost + ')' : '');
                if (fairA) fairA.textContent = baf + 'c';
            }
        }
    } catch(e) {}
}

// Power devig in JS for display
function powerDevig(homeOdds, awayOdds) {
    const ih = 1.0 / homeOdds, ia = 1.0 / awayOdds;
    let lo = 1.0, hi = 10.0;
    for (let i = 0; i < 100; i++) {
        const mid = (lo + hi) / 2.0;
        const total = Math.pow(ih, mid) + Math.pow(ia, mid);
        if (total > 1.0) lo = mid; else hi = mid;
    }
    const k = (lo + hi) / 2.0;
    const fh = Math.pow(ih, k), fa = Math.pow(ia, k);
    const s = fh + fa;
    return {home: fh / s, away: fa / s};
}

// ── Screen Track ──
function toggleScreenTrack() {
    const section = document.getElementById('screen-trade-section');
    const btn = document.getElementById('screen-track-btn');
    const tradeBtn = document.getElementById('screen-trade-btn');
    const modeBtn = document.getElementById('screen-mode-btn');
    if (section.style.display === 'none') {
        section.style.display = '';
        tradeBtn.style.display = '';
        modeBtn.style.display = '';
        btn.textContent = 'HIDE SCREEN'; btn.style.background = '#b71c1c'; btn.style.color = '#ff5252'; btn.style.borderColor = '#ff5252';
        if (!sctStreamRunning) sctToggleStream();
        fetch('/api/st_start', {method: 'POST'});
        if (!screenTradePoll) screenTradePoll = setInterval(sctPollAll, 60);
    } else {
        section.style.display = 'none';
        btn.textContent = 'SCREEN TRACK'; btn.style.background = '#1a237e'; btn.style.color = '#7986cb'; btn.style.borderColor = '#7986cb';
        if (screenTrading) stopScreenTrade();
        tradeBtn.style.display = 'none';
        modeBtn.style.display = 'none';
    }
}

function sctAddCurrentGame() {
    if (!currentGame) { alert('Select a match first'); return; }
    const key = sctGameKey(currentGame);
    if (sctTrackedGames[key]) return;
    sctTrackedGames[key] = {
        game: {...currentGame},
        lastHomeOdds: null,
        lastAwayOdds: null,
        sideFilter: 'both',
        boost: 0,
    };
    sctRenderTrackedGames();
    sctRenderMarkButtons();
}

function sctRemoveGame(key) {
    fetch('/api/st_remove_game', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({game_key: key}),
    });
    delete sctSubRegions[key + '_home'];
    delete sctSubRegions[key + '_away'];
    delete sctSubRegions[key + '_home_label'];
    delete sctSubRegions[key + '_away_label'];
    delete sctTrackedGames[key];
    sctRenderTrackedGames();
    sctRenderMarkButtons();
    sctRedrawSbCanvas();
    if (Object.keys(sctTrackedGames).length === 0) {
        if (screenTrading) stopScreenTrade();
    }
}

function sctRenderTrackedGames() {
    const el = document.getElementById('sct-tracked-games');
    if (!el) return;
    const keys = Object.keys(sctTrackedGames);
    if (keys.length === 0) { el.innerHTML = ''; return; }
    let html = '';
    for (const key of keys) {
        const g = sctTrackedGames[key];
        const sf = g.sideFilter || 'both';
        const safeKey = key.replace(/\\|/g, '_');
        const activeStyle = 'background:#1b5e20;color:#66bb6a;border:1px solid #66bb6a;';
        const inactiveStyle = 'background:#333;color:#888;border:1px solid #555;';
        const btnBase = 'padding:1px 6px;border-radius:3px;cursor:pointer;font-size:10px;';
        html += '<div style="display:flex;align-items:center;gap:10px;padding:5px 8px;background:#1a1a2e;border-radius:4px;margin-bottom:3px;font-size:12px;border:1px solid #333;">'
            + '<span style="color:#e0e0e0;font-weight:bold;min-width:180px;">' + g.game.home + ' vs ' + g.game.away + '</span>'
            + '<span style="color:#4caf50;">' + g.game.home + ': <b id="sct-tg-fair-' + safeKey + '-h">—</b>'
            + ' (<span id="sct-tg-raw-' + safeKey + '-h">—</span>)</span>'
            + '<span style="color:#ef5350;">' + g.game.away + ': <b id="sct-tg-fair-' + safeKey + '-a">—</b>'
            + ' (<span id="sct-tg-raw-' + safeKey + '-a">—</span>)</span>'
            + '<span style="display:flex;gap:3px;margin-left:8px;">'
            + '<button data-side-key="' + key + '" data-side="both" '
            + 'style="' + btnBase + (sf === 'both' ? activeStyle : inactiveStyle) + '">Both</button>'
            + '<button data-side-key="' + key + '" data-side="home" '
            + 'style="' + btnBase + (sf === 'home' ? activeStyle : inactiveStyle) + '">' + g.game.home + '</button>'
            + '<button data-side-key="' + key + '" data-side="away" '
            + 'style="' + btnBase + (sf === 'away' ? activeStyle : inactiveStyle) + '">' + g.game.away + '</button>'
            + '</span>'
            + '<span style="display:flex;align-items:center;gap:2px;margin-left:8px;">'
            + '<button data-boost-key="' + key + '" data-boost-delta="-1" '
            + 'style="' + btnBase + 'background:#333;color:#ef5350;border:1px solid #555;">−1</button>'
            + '<span id="sct-tg-boost-' + safeKey + '" style="color:#fff;min-width:36px;text-align:center;font-size:11px;">'
            + (g.boost > 0 ? '+' + g.boost : g.boost) + '%</span>'
            + '<button data-boost-key="' + key + '" data-boost-delta="1" '
            + 'style="' + btnBase + 'background:#333;color:#4caf50;border:1px solid #555;">+1</button>'
            + '</span>'
            + '<button data-remove-key="' + key + '" '
            + 'style="margin-left:auto;background:#333;border:1px solid #555;color:#ef5350;padding:2px 8px;border-radius:3px;cursor:pointer;font-size:11px;">✕</button>'
            + '</div>';
    }
    el.innerHTML = html;
    el.querySelectorAll('[data-side-key]').forEach(function(btn) {
        btn.addEventListener('click', function() {
            sctSetSideFilter(btn.dataset.sideKey, btn.dataset.side);
        });
    });
    el.querySelectorAll('[data-boost-key]').forEach(function(btn) {
        btn.addEventListener('click', function() {
            sctAdjustBoost(btn.dataset.boostKey, parseInt(btn.dataset.boostDelta));
        });
    });
}

function sctAdjustBoost(key, delta) {
    const g = sctTrackedGames[key];
    if (!g) return;
    g.boost = Math.max(-20, Math.min(20, (g.boost || 0) + delta));
    const safeKey = key.replace(/\\|/g, '_');
    const el = document.getElementById('sct-tg-boost-' + safeKey);
    if (el) {
        el.textContent = (g.boost > 0 ? '+' + g.boost : g.boost) + '%';
        el.style.color = g.boost > 0 ? '#4caf50' : g.boost < 0 ? '#ef5350' : '#fff';
    }
    sctLog('Boost ' + key + ' home ' + (g.boost > 0 ? '+' : '') + g.boost + '%');
}

function playFillSound() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.frequency.value = 880;
        osc.type = 'sine';
        gain.gain.value = 0.3;
        osc.start();
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3);
        osc.stop(ctx.currentTime + 0.3);
    } catch(e) {}
}

function playShiftAlarm() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        for (let i = 0; i < 3; i++) {
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.frequency.value = 440;
            osc.type = 'square';
            gain.gain.value = 0.4;
            osc.start(ctx.currentTime + i * 0.25);
            gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + i * 0.25 + 0.2);
            osc.stop(ctx.currentTime + i * 0.25 + 0.2);
        }
    } catch(e) {}
}

function sctSetSideFilter(key, side) {
    if (sctTrackedGames[key]) {
        sctTrackedGames[key].sideFilter = side;
        sctRenderTrackedGames();
    }
}

function sctLog(msg) {
    const el = document.getElementById('sct-trade-log');
    if (!el) return;
    const now = new Date();
    const ts = now.toLocaleTimeString('en-US', {hour12: false}) + '.' + String(now.getMilliseconds()).padStart(3, '0');
    el.innerHTML = '<div><span style="color:#555;">' + ts + '</span> ' + msg + '</div>' + el.innerHTML;
    if (el.children.length > 50) el.removeChild(el.lastChild);
}

function toggleScreenTradeMode() {
    screenTradeMode = screenTradeMode === 'ioc' ? 'maker' : 'ioc';
    const btn = document.getElementById('screen-mode-btn');
    if (screenTradeMode === 'maker') {
        btn.textContent = 'MAKER'; btn.style.background = '#1a237e'; btn.style.color = '#7986cb'; btn.style.borderColor = '#7986cb';
    } else {
        btn.textContent = 'IOC'; btn.style.background = '#333'; btn.style.color = '#ffa726'; btn.style.borderColor = '#ffa726';
    }
}

// ── Screen Trade (toggle trading on/off) ──
function toggleScreenTrade() {
    if (screenTrading) stopScreenTrade(); else startScreenTrade();
}

function startScreenTrade() {
    screenTrading = true;
    for (const g of Object.values(sctTrackedGames)) { g.lastHomeOdds = null; g.lastAwayOdds = null; g.baseHomeLabel = null; g.baseAwayLabel = null; g.tradeBusy = false; g.locked = false; if (g.makerTimer) { clearTimeout(g.makerTimer); g.makerTimer = null; } g.pendingMakerFair = null; }
    const btn = document.getElementById('screen-trade-btn');
    btn.textContent = 'TRADE OFF'; btn.style.background = '#b71c1c'; btn.style.color = '#ff5252'; btn.style.borderColor = '#ff5252';
    const log = document.getElementById('sct-trade-log');
    if (log) { log.style.display = ''; log.innerHTML = ''; }
    sctLog('<span style="color:#66bb6a;">TRADING ON</span> mode=' + screenTradeMode);
}

async function stopScreenTrade() {
    screenTrading = false;
    const btn = document.getElementById('screen-trade-btn');
    btn.textContent = 'TRADE ON'; btn.style.background = '#1b5e20'; btn.style.color = '#66bb6a'; btn.style.borderColor = '#66bb6a';
    sctLog('<span style="color:#ff5252;">TRADING OFF</span>');
    try {
        const promises = [];
        for (const g of Object.values(sctTrackedGames)) {
            if (g.makerTimer) { clearTimeout(g.makerTimer); g.makerTimer = null; }
            g.pendingMakerFair = null;
            const gm = g.game;
            if (gm.tickers && gm.tickers.length) {
                promises.push(fetch('/api/cancel_all', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({tickers: gm.tickers}),
                }));
            }
            if (gm.polySeriesTickers && gm.polySeriesTickers.length) {
                const tokenIds = [];
                for (const t of gm.polySeriesTickers) { tokenIds.push(t.token_a); tokenIds.push(t.token_b); }
                promises.push(fetch('/api/poly_cancel_all', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({token_ids: tokenIds}),
                }));
            }
        }
        await Promise.all(promises);
    } catch(e) {}
}

let lastWsFillCount = 0;
// ── Poll loop: single state fetch, process all games ──
async function sctPollAll() {
    const entries = Object.entries(sctTrackedGames);
    if (entries.length === 0) return;

    // Check WS fills in background (non-blocking)
    if (screenTrading) {
        fetch('/api/ws_status').then(r => r.json()).then(ws => {
            const el = document.getElementById('ws-status');
            if (el) {
                el.textContent = ws.connected ? 'WS:ON' : 'WS:OFF';
                el.style.color = ws.connected ? '#66bb6a' : '#ef5350';
            }
            if (ws.fills && ws.fills.length > lastWsFillCount) {
                const newFills = ws.fills.slice(lastWsFillCount);
                lastWsFillCount = ws.fills.length;
                for (const f of newFills) {
                    const tag = f.is_taker ? 'TAKER' : 'MAKER';
                    sctLog('<span style="color:#ff9800;">[WS FILL] ' + tag + ' ' + f.side + ' ' + f.count + '@' + f.price + ' ' + f.ticker.split('-').pop() + '</span>');
                    playFillSound();
                }
            }
        }).catch(() => {});
    }

    try {
        const resp = await fetch('/api/st_state');
        const state = await resp.json();
        const odds = state.odds || {};
        const labels = state.labels || {};
        const tradePromises = [];

        if (screenTrading) {
            for (const [key, g] of entries) {
                const hl = labels[key + '_home_label'];
                const al = labels[key + '_away_label'];
                if (hl != null && g.baseHomeLabel == null) { g.baseHomeLabel = hl; sctLog('Label baseline set: <span style="color:#ffa726;">' + hl + '</span>'); }
                if (al != null && g.baseAwayLabel == null) { g.baseAwayLabel = al; sctLog('Label baseline set: <span style="color:#ffa726;">' + al + '</span>'); }
                if (g.baseHomeLabel != null && hl != null && hl !== g.baseHomeLabel) {
                    sctLog('<span style="color:#ff5252;">LABEL SHIFT: "' + hl + '" != "' + g.baseHomeLabel + '" — STOPPING</span>');
                    stopScreenTrade(); playShiftAlarm(); return;
                }
                if (g.baseAwayLabel != null && al != null && al !== g.baseAwayLabel) {
                    sctLog('<span style="color:#ff5252;">LABEL SHIFT: "' + al + '" != "' + g.baseAwayLabel + '" — STOPPING</span>');
                    stopScreenTrade(); playShiftAlarm(); return;
                }
            }
        }

        for (const [key, g] of entries) {
            const ho = odds[key + '_home'];
            const ao = odds[key + '_away'];
            const safeKey = key.replace(/\\|/g, '_');

            if ((ho == null || ao == null) && screenTrading && g.lastHomeOdds != null) {
                const locked = ho == null && ao == null ? 'BOTH' : (ho == null ? 'HOME' : 'AWAY');
                if (!g.locked) {
                    sctLog('<span style="color:#ff5252;font-weight:bold;">LOCKED (' + locked + ') — EMERGENCY CANCEL ' + key + '</span>');
                    g.locked = true;
                }
                if (g.makerTimer) { clearTimeout(g.makerTimer); g.makerTimer = null; }
                g.pendingMakerFair = null;
                g.tradeBusy = true;
                const gm = g.game;
                if (gm.tickers && gm.tickers.length) {
                    fetch('/api/cancel_all', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({tickers: gm.tickers}),
                    }).then(() => {
                        sctLog('<span style="color:#ffa726;">CANCEL CONFIRMED for ' + key + '</span>');
                    }).catch(() => {}).finally(() => { g.tradeBusy = false; });
                } else {
                    g.tradeBusy = false;
                }
            }
            if (ho != null && ao != null) {
                if (g.locked) {
                    sctLog('<span style="color:#66bb6a;font-weight:bold;">UNLOCKED ' + key + '</span> odds=' + ho.toFixed(2) + '/' + ao.toFixed(2) + ' anchor=' + (g.lastHomeOdds != null ? g.lastHomeOdds.toFixed(2) : '?') + '/' + (g.lastAwayOdds != null ? g.lastAwayOdds.toFixed(2) : '?'));
                    g.locked = false;
                }
                const fair = powerDevig(ho, ao);
                const boost = g.boost || 0;
                const hf = Math.max(1, Math.min(99, Math.round(fair.home * 100) + boost));
                const af = 100 - hf;
                const rawH = document.getElementById('sct-tg-raw-' + safeKey + '-h');
                const rawA = document.getElementById('sct-tg-raw-' + safeKey + '-a');
                const fairH = document.getElementById('sct-tg-fair-' + safeKey + '-h');
                const fairA = document.getElementById('sct-tg-fair-' + safeKey + '-a');
                if (rawH) rawH.textContent = ho.toFixed(2);
                if (rawA) rawA.textContent = ao.toFixed(2);
                if (fairH) fairH.textContent = hf + 'c' + (boost ? ' (' + (boost > 0 ? '+' : '') + boost + ')' : '');
                if (fairA) fairA.textContent = af + 'c';

                if (screenTrading && (ho !== g.lastHomeOdds || ao !== g.lastAwayOdds)) {
                    let tradeSide = 'both';
                    if (screenTradeMode === 'ioc') {
                        if (g.lastHomeOdds != null && g.lastAwayOdds != null) {
                            const homeShortened = ho < g.lastHomeOdds;
                            const awayShortened = ao < g.lastAwayOdds;
                            if (homeShortened && !awayShortened) tradeSide = 'home';
                            else if (awayShortened && !homeShortened) tradeSide = 'away';
                            else if (!homeShortened && !awayShortened) tradeSide = 'none';
                            sctLog('Odds moved ' + ho.toFixed(2) + '/' + ao.toFixed(2)
                                + (homeShortened ? ' <span style="color:#4caf50;">H&darr;</span>' : '')
                                + (awayShortened ? ' <span style="color:#ef5350;">A&darr;</span>' : '')
                                + ' &rarr; ' + tradeSide);
                        } else {
                            tradeSide = 'none';
                            sctLog('Baseline set ' + ho.toFixed(2) + '/' + ao.toFixed(2) + ' fair=' + hf + '/' + af);
                        }
                    } else {
                        sctLog('Odds moved ' + ho.toFixed(2) + '/' + ao.toFixed(2) + ' fair=' + hf + '/' + af + ' &rarr; reprice');
                    }
                    g.lastHomeOdds = ho; g.lastAwayOdds = ao;
                    const sf = g.sideFilter || 'both';
                    if (sf !== 'both') {
                        if (tradeSide === 'both') tradeSide = sf;
                        else if (tradeSide !== sf) tradeSide = 'none';
                    }
                    if (tradeSide !== 'none' && screenTradeMode === 'maker') {
                        const gm = g.game;
                        if (gm.tickers && gm.tickers.length) {
                            fetch('/api/cancel_all', {
                                method: 'POST', headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({tickers: gm.tickers}),
                            }).catch(() => {});
                            sctLog('<span style="color:#ffb74d;">CANCELED — reposting</span>');
                        }
                        if (g.makerTimer) clearTimeout(g.makerTimer);
                        g.pendingMakerFair = {hf, af, tradeSide};
                        g.makerTimer = setTimeout(() => {
                            g.makerTimer = null;
                            const pm = g.pendingMakerFair;
                            if (!pm || g.tradeBusy) return;
                            g.tradeBusy = true;
                            const contracts = parseInt(document.getElementById('contracts').value);
                            const spread = parseInt(document.getElementById('spread-cents').value);
                            sctLog('<span style="color:#7986cb;">POSTING MAKER</span> side=' + pm.tradeSide + ' fair=' + pm.hf + '/' + pm.af);
                            fetch('/api/screen_trade', {
                                method: 'POST', headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({
                                    home: gm.home, away: gm.away,
                                    tickers: gm.tickers,
                                    poly_tickers: gm.polySeriesTickers,
                                    contracts: contracts, spread_cents: spread,
                                    game_key: key,
                                    trade_side: pm.tradeSide,
                                    mode: 'maker',
                                    home_fair_cents: pm.hf,
                                }),
                            }).then(r => r.json()).then(d => {
                                if (d.orders && d.orders.length) {
                                    renderResults(d);
                                    const fills = d.orders.filter(o => o.status === 'filled' || (o.filled && o.filled > 0));
                                    if (fills.length) {
                                        playFillSound();
                                        fills.forEach(o => sctLog('<span style="color:#66bb6a;">FILLED</span> ' + o.team + ' @' + o.price + 'c'));
                                    }
                                }
                            }).catch(e => { sctLog('<span style="color:#ff5252;">ERROR: ' + e + '</span>'); }).finally(() => { g.tradeBusy = false; });
                        }, 300);
                    } else if (tradeSide !== 'none' && !g.tradeBusy) {
                        g.tradeBusy = true;
                        const contracts = parseInt(document.getElementById('contracts').value);
                        const spread = parseInt(document.getElementById('spread-cents').value);
                        const gm = g.game;
                        sctLog('<span style="color:#7986cb;">SENDING ' + screenTradeMode.toUpperCase() + '</span> side=' + tradeSide + ' fair=' + hf + '/' + af);
                        fetch('/api/screen_trade', {
                            method: 'POST', headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                home: gm.home, away: gm.away,
                                tickers: gm.tickers,
                                poly_tickers: gm.polySeriesTickers,
                                contracts: contracts, spread_cents: spread,
                                game_key: key,
                                trade_side: tradeSide,
                                mode: screenTradeMode,
                                home_fair_cents: hf,
                            }),
                        }).then(r => r.json()).then(d => {
                            if (d.orders && d.orders.length) {
                                renderResults(d);
                                const fills = d.orders.filter(o => o.status === 'filled' || (o.filled && o.filled > 0));
                                if (fills.length) {
                                    playFillSound();
                                    fills.forEach(o => sctLog('<span style="color:#66bb6a;">FILLED</span> ' + o.team + ' @' + o.price + 'c'));
                                }
                            }
                        }).catch(e => { sctLog('<span style="color:#ff5252;">ERROR: ' + e + '</span>'); }).finally(() => { g.tradeBusy = false; });
                    } else if (tradeSide === 'none') {
                        sctLog('<span style="color:#555;">no trade (side=none)</span>');
                    } else if (g.tradeBusy) {
                        sctLog('<span style="color:#555;">skipped (busy)</span>');
                    }
                }
            }
        }
    } catch(e) {}
}

// ── Esports Screen Trader ─────────────────────────────────
let espScreenData = null;
let espStreamRunning = false;
let espStreamRAF = null;
let espStreamFetching = false;
let espStreamFrameCount = 0;
let espStreamLastFpsTime = 0;
let espScoreboard = null;
let espSubRegions = {};
let espRegionMode = null;
let espSubMode = null;
let espDrawStart = null;
let espSbDrawStart = null;
let espSbPreviewInterval = null;
let espZoom = 1;
let espPanX = 0, espPanY = 0;
let espIsPanning = false;
let espPanLast = null;

let espTrading = false;
let espTradePoll = null;
let espTradeMode = 'ioc';
let espTrackedGames = {};
let espLastWsFillCount = 0;

function espUpdateTransform() {
    const inner = document.getElementById('esp-screen-inner');
    if (inner) inner.style.transform = 'scale(' + espZoom + ') translate(' + espPanX + 'px,' + espPanY + 'px)';
}

function espGetCanvasCoords(e, canvasId) {
    const canvas = document.getElementById(canvasId || 'esp-screen-canvas');
    const rect = canvas.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return {x: 0, y: 0};
    return {
        x: (e.clientX - rect.left) * (canvas.width / rect.width),
        y: (e.clientY - rect.top) * (canvas.height / rect.height),
    };
}

async function espStreamFrame() {
    if (!espStreamRunning || espStreamFetching) return;
    espStreamFetching = true;
    try {
        const resp = await fetch('/api/st_screenshot', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({fast: true}),
        });
        const data = await resp.json();
        if (!data.error) {
            espScreenData = data;
            const img = document.getElementById('esp-screen-img');
            img.src = 'data:image/' + (data.format || 'png') + ';base64,' + data.base64;
            img.onload = () => {
                img.style.display = 'block';
                const canvas = document.getElementById('esp-screen-canvas');
                if (canvas.width !== img.naturalWidth || canvas.height !== img.naturalHeight) {
                    canvas.width = img.naturalWidth; canvas.height = img.naturalHeight;
                }
                espRedrawCanvas();
                espStreamFrameCount++;
                const now = performance.now();
                if (now - espStreamLastFpsTime > 2000) {
                    const fps = (espStreamFrameCount / ((now - espStreamLastFpsTime) / 1000)).toFixed(1);
                    document.getElementById('esp-stream-fps').textContent = fps + ' fps';
                    espStreamFrameCount = 0; espStreamLastFpsTime = now;
                }
            };
        }
    } catch(e) {}
    espStreamFetching = false;
    if (espStreamRunning) espStreamRAF = setTimeout(espStreamFrame, 0);
}

function espToggleStream() {
    const container = document.getElementById('esp-screenshot-container');
    if (espStreamRunning) {
        espStreamRunning = false;
        if (espStreamRAF) { clearTimeout(espStreamRAF); espStreamRAF = null; }
        container.style.display = 'none';
        document.getElementById('esp-stream-toggle').textContent = 'Live View';
        document.getElementById('esp-stream-fps').textContent = '';
    } else {
        espStreamRunning = true;
        espStreamLastFpsTime = performance.now(); espStreamFrameCount = 0;
        container.style.display = '';
        document.getElementById('esp-stream-toggle').textContent = 'Hide View';
        espStreamFrame();
    }
}

function espSetRegionMode(mode) {
    if (!espStreamRunning) espToggleStream();
    espRegionMode = mode;
    document.querySelectorAll('.esp-region-btn').forEach(b => {
        b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
    });
    const btn = document.getElementById('esp-rgn-btn-' + mode);
    if (btn) { btn.style.background = '#e040fb'; btn.style.color = '#000'; btn.style.borderColor = '#e040fb'; }
    document.getElementById('esp-screenshot-container').style.cursor = 'crosshair';
}

function espRedrawCanvas() {
    const canvas = document.getElementById('esp-screen-canvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (!espScreenData) return;
    const sx = canvas.width / espScreenData.width;
    const sy = canvas.height / espScreenData.height;
    if (espScoreboard) {
        ctx.strokeStyle = '#e040fb'; ctx.lineWidth = 2;
        ctx.strokeRect(espScoreboard.x * sx, espScoreboard.y * sy, espScoreboard.w * sx, espScoreboard.h * sy);
        ctx.fillStyle = '#e040fb'; ctx.font = '12px monospace';
        ctx.fillText('Odds Region', espScoreboard.x * sx + 2, espScoreboard.y * sy - 4);
    }
    if (espDrawStart && espRegionMode) {
        ctx.strokeStyle = '#e040fb'; ctx.lineWidth = 2; ctx.setLineDash([4, 4]);
        ctx.strokeRect(espDrawStart.sx, espDrawStart.sy, espDrawStart.dx || 0, espDrawStart.dy || 0);
        ctx.setLineDash([]);
    }
}

function espClearAll() {
    espScoreboard = null; espSubRegions = {};
    espRegionMode = null; espSubMode = null;
    espTrackedGames = {};
    fetch('/api/st_clear_regions', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({}),
    });
    const canvas = document.getElementById('esp-screen-canvas');
    if (canvas) canvas.getContext('2d').clearRect(0, 0, canvas.width, canvas.height);
    document.getElementById('esp-sb-section').style.display = 'none';
    if (espSbPreviewInterval) { clearInterval(espSbPreviewInterval); espSbPreviewInterval = null; }
    espRenderTrackedGames();
    espRenderMarkButtons();
}

// Screenshot container mouse handlers
(function() {
    const container = document.getElementById('esp-screenshot-container');
    if (!container) return;
    container.addEventListener('wheel', function(e) {
        e.preventDefault();
        espZoom = Math.max(1, Math.min(6, espZoom + (e.deltaY > 0 ? -0.3 : 0.3)));
        if (espZoom <= 1) { espZoom = 1; espPanX = 0; espPanY = 0; }
        espUpdateTransform();
    }, {passive: false});
    container.addEventListener('mousedown', function(e) {
        if (!espScreenData) return;
        e.preventDefault();
        if (espRegionMode === 'scoreboard') {
            espDrawStart = espGetCanvasCoords(e);
            espDrawStart = {sx: espDrawStart.x, sy: espDrawStart.y, dx: 0, dy: 0};
        } else {
            espIsPanning = true; espPanLast = {x: e.clientX, y: e.clientY};
            container.style.cursor = 'grabbing';
        }
    });
    container.addEventListener('mousemove', function(e) {
        if (espDrawStart) {
            const c = espGetCanvasCoords(e);
            espDrawStart.dx = c.x - espDrawStart.sx; espDrawStart.dy = c.y - espDrawStart.sy;
            espRedrawCanvas();
        } else if (espIsPanning && espPanLast) {
            espPanX += (e.clientX - espPanLast.x) / espZoom;
            espPanY += (e.clientY - espPanLast.y) / espZoom;
            espPanLast = {x: e.clientX, y: e.clientY}; espUpdateTransform();
        }
    });
    container.addEventListener('mouseup', function(e) {
        if (espIsPanning) { espIsPanning = false; espPanLast = null;
            container.style.cursor = espRegionMode ? 'crosshair' : 'default'; return; }
        if (!espDrawStart || espRegionMode !== 'scoreboard' || !espScreenData) return;
        const canvas = document.getElementById('esp-screen-canvas');
        const c = espGetCanvasCoords(e);
        let x1 = espDrawStart.sx, y1 = espDrawStart.sy, x2 = c.x, y2 = c.y;
        if (Math.abs(x2-x1) < 10 || Math.abs(y2-y1) < 10) { espDrawStart = null; return; }
        const scX = espScreenData.width / canvas.width, scY = espScreenData.height / canvas.height;
        espScoreboard = {
            x: Math.round(Math.min(x1,x2)*scX), y: Math.round(Math.min(y1,y2)*scY),
            w: Math.round(Math.abs(x2-x1)*scX), h: Math.round(Math.abs(y2-y1)*scY),
        };
        espDrawStart = null; espRegionMode = null;
        document.querySelectorAll('.esp-region-btn').forEach(b => {
            b.style.background = '#333'; b.style.color = '#e0e0e0'; b.style.borderColor = '#555';
        });
        container.style.cursor = 'default';
        espRedrawCanvas();
        fetch('/api/st_set_scoreboard', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(espScoreboard),
        });
        espShowSbPreview();
    });
    container.addEventListener('mouseleave', function() {
        if (espIsPanning) { espIsPanning = false; espPanLast = null; }
    });
})();

function espShowSbPreview() {
    document.getElementById('esp-sb-section').style.display = '';
    espRefreshSbPreview();
    if (espSbPreviewInterval) clearInterval(espSbPreviewInterval);
    espSbPreviewInterval = setInterval(espRefreshSbPreview, 150);
}

async function espRefreshSbPreview() {
    try {
        const resp = await fetch('/api/st_scoreboard_capture', {method: 'POST'});
        const data = await resp.json();
        if (data.error) return;
        const img = document.getElementById('esp-sb-img');
        img.src = 'data:image/jpeg;base64,' + data.base64;
        img.onload = () => {
            const canvas = document.getElementById('esp-sb-canvas');
            if (canvas.width !== img.naturalWidth || canvas.height !== img.naturalHeight) {
                canvas.width = img.naturalWidth; canvas.height = img.naturalHeight;
            }
            espRedrawSbCanvas();
        };
    } catch(e) {}
}

function espSetSubMode(mode) {
    espSubMode = mode;
    espRenderMarkButtons();
    document.getElementById('esp-sb-preview').style.cursor = 'crosshair';
}

function espRedrawSbCanvas() {
    const canvas = document.getElementById('esp-sb-canvas');
    if (!canvas || !espScoreboard) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const sx = canvas.width / espScoreboard.w, sy = canvas.height / espScoreboard.h;
    for (const [key, r] of Object.entries(espSubRegions)) {
        const isLabel = key.endsWith('_label');
        const isHome = key.includes('_home');
        const color = isLabel ? '#ffa726' : (isHome ? '#4caf50' : '#ef5350');
        const gameKey = key.replace(/_(?:home|away)(?:_label)?$/, '');
        const g = espTrackedGames[gameKey];
        const label = isLabel ? 'Label' : (isHome ? (g ? g.game.home : 'Home') : (g ? g.game.away : 'Away'));
        ctx.strokeStyle = color; ctx.lineWidth = 2;
        ctx.strokeRect(r.x*sx, r.y*sy, r.w*sx, r.h*sy);
        ctx.fillStyle = color; ctx.font = '11px monospace';
        ctx.fillText(label, r.x*sx+2, r.y*sy-3);
    }
    if (espSbDrawStart && espSubMode) {
        const isLabel = espSubMode.endsWith('_label');
        const isHome = espSubMode.includes('_home');
        ctx.strokeStyle = isLabel ? '#ffa726' : (isHome ? '#4caf50' : '#ef5350'); ctx.lineWidth = 2;
        ctx.setLineDash([4,4]);
        ctx.strokeRect(espSbDrawStart.sx, espSbDrawStart.sy, espSbDrawStart.dx||0, espSbDrawStart.dy||0);
        ctx.setLineDash([]);
    }
}

// Scoreboard preview mouse handlers
(function() {
    const preview = document.getElementById('esp-sb-preview');
    if (!preview) return;
    preview.addEventListener('mousedown', function(e) {
        if (!espSubMode) return; e.preventDefault();
        const c = espGetCanvasCoords(e, 'esp-sb-canvas');
        espSbDrawStart = {sx: c.x, sy: c.y, dx: 0, dy: 0};
    });
    preview.addEventListener('mousemove', function(e) {
        if (!espSbDrawStart) return;
        const c = espGetCanvasCoords(e, 'esp-sb-canvas');
        espSbDrawStart.dx = c.x - espSbDrawStart.sx; espSbDrawStart.dy = c.y - espSbDrawStart.sy;
        espRedrawSbCanvas();
    });
    preview.addEventListener('mouseup', function(e) {
        if (!espSbDrawStart || !espSubMode || !espScoreboard) return;
        const canvas = document.getElementById('esp-sb-canvas');
        const c = espGetCanvasCoords(e, 'esp-sb-canvas');
        let x1 = espSbDrawStart.sx, y1 = espSbDrawStart.sy, x2 = c.x, y2 = c.y;
        if (Math.abs(x2-x1) < 3 || Math.abs(y2-y1) < 3) { espSbDrawStart = null; return; }
        const scX = espScoreboard.w / canvas.width, scY = espScoreboard.h / canvas.height;
        const region = {
            x: Math.round(Math.min(x1,x2)*scX), y: Math.round(Math.min(y1,y2)*scY),
            w: Math.round(Math.abs(x2-x1)*scX), h: Math.round(Math.abs(y2-y1)*scY),
        };
        espSubRegions[espSubMode] = region;
        espSbDrawStart = null;
        fetch('/api/st_set_sub_region', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({name: espSubMode, region: region}),
        });
        if (espSubMode.endsWith('_home')) {
            const awayKey = espSubMode.replace(/_home$/, '_away');
            if (!espSubRegions[awayKey]) {
                espSubMode = awayKey;
                espRedrawSbCanvas();
                espRenderMarkButtons();
                return;
            }
        }
        espSubMode = null;
        espRedrawSbCanvas();
        espRenderMarkButtons();
    });
})();

function espRenderMarkButtons() {
    const el = document.getElementById('esp-mark-buttons');
    if (!el) return;
    const keys = Object.keys(espTrackedGames);
    if (keys.length === 0) { el.innerHTML = ''; return; }
    let html = '';
    for (const key of keys) {
        const g = espTrackedGames[key];
        const homeKey = key + '_home';
        const awayKey = key + '_away';
        const homeLabelKey = key + '_home_label';
        const awayLabelKey = key + '_away_label';
        const homeSet = !!espSubRegions[homeKey];
        const awaySet = !!espSubRegions[awayKey];
        const homeLabelSet = !!espSubRegions[homeLabelKey];
        const awayLabelSet = !!espSubRegions[awayLabelKey];
        const homeActive = espSubMode === homeKey;
        const awayActive = espSubMode === awayKey;
        const homeLabelActive = espSubMode === homeLabelKey;
        const awayLabelActive = espSubMode === awayLabelKey;
        html += '<span style="color:#777;font-size:10px;margin-right:2px;">' + g.game.home + ' v ' + g.game.away + ':</span>';
        html += '<button class="esp-sub-btn" data-espsubkey="' + homeKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (homeActive ? '#4caf50' : (homeSet ? '#1b5e20' : '#333'))
            + ';border:1px solid #4caf50;color:' + (homeActive ? '#000' : '#4caf50') + ';border-radius:3px;cursor:pointer;">'
            + g.game.home + (homeSet ? ' &#10003;' : '') + '</button>';
        html += '<button class="esp-sub-btn" data-espsubkey="' + homeLabelKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (homeLabelActive ? '#ffa726' : (homeLabelSet ? '#e65100' : '#333'))
            + ';border:1px solid #ffa726;color:' + (homeLabelActive ? '#000' : '#ffa726') + ';border-radius:3px;cursor:pointer;">'
            + 'Lbl' + (homeLabelSet ? ' &#10003;' : '') + '</button>';
        html += '<button class="esp-sub-btn" data-espsubkey="' + awayKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (awayActive ? '#ef5350' : (awaySet ? '#b71c1c' : '#333'))
            + ';border:1px solid #ef5350;color:' + (awayActive ? '#000' : '#ef5350') + ';border-radius:3px;cursor:pointer;">'
            + g.game.away + (awaySet ? ' &#10003;' : '') + '</button>';
        html += '<button class="esp-sub-btn" data-espsubkey="' + awayLabelKey + '" '
            + 'style="padding:2px 6px;font-size:10px;background:' + (awayLabelActive ? '#ffa726' : (awayLabelSet ? '#e65100' : '#333'))
            + ';border:1px solid #ffa726;color:' + (awayLabelActive ? '#000' : '#ffa726') + ';border-radius:3px;cursor:pointer;">'
            + 'Lbl' + (awayLabelSet ? ' &#10003;' : '') + '</button>';
        html += '<span style="margin-right:10px;"></span>';
    }
    html += '<button onclick="espTestAllOCR()" style="padding:2px 6px;font-size:10px;background:#555;border:1px solid #666;color:#ccc;border-radius:3px;cursor:pointer;">Test OCR</button>';
    el.innerHTML = html;
}

(function() {
    document.getElementById('esp-mark-buttons').addEventListener('click', function(e) {
        const btn = e.target.closest('[data-espsubkey]');
        if (btn) espSetSubMode(btn.dataset.espsubkey);
    });
    document.getElementById('esp-tracked-games').addEventListener('click', function(e) {
        const btn = e.target.closest('[data-espremove-key]');
        if (btn) { e.stopPropagation(); espRemoveGame(btn.dataset.espremoveKey); }
    });
})();

async function espTestAllOCR() {
    try {
        const resp = await fetch('/api/st_capture_once', {method: 'POST'});
        const data = await resp.json();
        const odds = data.odds || {};
        for (const [key, g] of Object.entries(espTrackedGames)) {
            const safeKey = key.replace(/[|\\]/g, '_');
            const ho = odds[key + '_home'], ao = odds[key + '_away'];
            const rawH = document.getElementById('esp-tg-raw-' + safeKey + '-h');
            const rawA = document.getElementById('esp-tg-raw-' + safeKey + '-a');
            if (rawH) rawH.textContent = ho != null ? ho.toFixed(2) : 'fail';
            if (rawA) rawA.textContent = ao != null ? ao.toFixed(2) : 'fail';
            if (ho != null && ao != null) {
                const fair = powerDevig(ho, ao);
                const boost = g.boost || 0;
                const bhf = Math.max(1, Math.min(99, Math.round(fair.home * 100) + boost));
                const baf = 100 - bhf;
                const fairH = document.getElementById('esp-tg-fair-' + safeKey + '-h');
                const fairA = document.getElementById('esp-tg-fair-' + safeKey + '-a');
                if (fairH) fairH.textContent = bhf + 'c' + (boost ? ' (' + (boost > 0 ? '+' : '') + boost + ')' : '');
                if (fairA) fairA.textContent = baf + 'c';
            }
        }
    } catch(e) {}
}

// ── Fetch games for selected esport ──
let espAvailableGames = [];

async function espOnEsportChange() {
    const esport = document.getElementById('esp-esport').value;
    const sel = document.getElementById('esp-game-select');
    const status = document.getElementById('esp-fetch-status');
    if (!esport) {
        sel.innerHTML = '<option value="">-- Pick esport first --</option>';
        espAvailableGames = [];
        return;
    }
    sel.innerHTML = '<option value="">Loading...</option>';
    status.textContent = 'Fetching markets...';
    status.style.color = '#ffa726';
    try {
        const resp = await fetch('/api/esports/fetch_games', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({esport: esport}),
        });
        const data = await resp.json();
        if (data.error) {
            sel.innerHTML = '<option value="">' + data.error + '</option>';
            status.textContent = data.error;
            status.style.color = '#ef5350';
            espAvailableGames = [];
            return;
        }
        espAvailableGames = data.games || [];
        if (espAvailableGames.length === 0) {
            sel.innerHTML = '<option value="">No matches found</option>';
            status.textContent = 'No matches found';
            status.style.color = '#ef5350';
            return;
        }
        let html = '<option value="">-- Select a match --</option>';
        for (let i = 0; i < espAvailableGames.length; i++) {
            const g = espAvailableGames[i];
            const kCount = g.tickers ? g.tickers.length : 0;
            const pCount = g.polySeriesTickers ? g.polySeriesTickers.length : 0;
            const platforms = [];
            if (kCount) platforms.push('K:' + kCount);
            if (pCount) platforms.push('P:' + pCount);
            const tag = platforms.length ? ' [' + platforms.join(' ') + ']' : '';
            html += '<option value="' + i + '">' + g.away + ' vs ' + g.home + tag + '</option>';
        }
        sel.innerHTML = html;
        status.textContent = espAvailableGames.length + ' matches';
        status.style.color = '#66bb6a';
    } catch(e) {
        sel.innerHTML = '<option value="">Fetch error</option>';
        status.textContent = 'Error: ' + e.message;
        status.style.color = '#ef5350';
        espAvailableGames = [];
    }
}

function espAddSelectedGame() {
    const sel = document.getElementById('esp-game-select');
    const idx = parseInt(sel.value);
    if (isNaN(idx) || !espAvailableGames[idx]) { alert('Select a match first'); return; }
    const g = espAvailableGames[idx];
    const esport = document.getElementById('esp-esport').value;
    const key = g.home + '|' + g.away;
    if (espTrackedGames[key]) { alert('Already added'); return; }

    espTrackedGames[key] = {
        game: {home: g.home, away: g.away, esport: esport, tickers: g.tickers || [], polySeriesTickers: g.polySeriesTickers || []},
        lastHomeOdds: null,
        lastAwayOdds: null,
        sideFilter: 'both',
        boost: 0,
    };
    espRenderTrackedGames();
    espRenderMarkButtons();

    if (!espTradePoll) {
        fetch('/api/st_start', {method: 'POST'});
        espTradePoll = setInterval(espPollAll, 60);
    }
}

function espRemoveGame(key) {
    fetch('/api/st_remove_game', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({game_key: key}),
    });
    delete espSubRegions[key + '_home'];
    delete espSubRegions[key + '_away'];
    delete espSubRegions[key + '_home_label'];
    delete espSubRegions[key + '_away_label'];
    delete espTrackedGames[key];
    espRenderTrackedGames();
    espRenderMarkButtons();
    espRedrawSbCanvas();
    if (Object.keys(espTrackedGames).length === 0 && espTrading) espStopTrade();
}

const ESP_ESPORT_LABELS = {valorant: 'VAL', lol: 'LoL', dota2: 'Dota'};
const ESP_ESPORT_COLORS = {valorant: '#ff4655', lol: '#c89b3c', dota2: '#e44d2e'};

function espRenderTrackedGames() {
    const el = document.getElementById('esp-tracked-games');
    if (!el) return;
    const keys = Object.keys(espTrackedGames);
    if (keys.length === 0) { el.innerHTML = ''; return; }
    let html = '';
    for (const key of keys) {
        const g = espTrackedGames[key];
        const sf = g.sideFilter || 'both';
        const safeKey = key.replace(/[|\\]/g, '_');
        const esport = g.game.esport || 'valorant';
        const espLabel = ESP_ESPORT_LABELS[esport] || esport;
        const espColor = ESP_ESPORT_COLORS[esport] || '#e040fb';
        const activeStyle = 'background:#1b5e20;color:#66bb6a;border:1px solid #66bb6a;';
        const inactiveStyle = 'background:#333;color:#888;border:1px solid #555;';
        const btnBase = 'padding:1px 6px;border-radius:3px;cursor:pointer;font-size:10px;';
        html += '<div style="display:flex;align-items:center;gap:10px;padding:5px 8px;background:#1a1a2e;border-radius:4px;margin-bottom:3px;font-size:12px;border:1px solid #333;">'
            + '<span style="background:' + espColor + ';color:#000;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:bold;">' + espLabel + '</span>'
            + '<span style="color:#e0e0e0;font-weight:bold;min-width:160px;">' + g.game.home + ' vs ' + g.game.away + '</span>'
            + '<span style="color:#4caf50;">' + g.game.home + ': <b id="esp-tg-fair-' + safeKey + '-h">—</b>'
            + ' (<span id="esp-tg-raw-' + safeKey + '-h">—</span>)</span>'
            + '<span style="color:#ef5350;">' + g.game.away + ': <b id="esp-tg-fair-' + safeKey + '-a">—</b>'
            + ' (<span id="esp-tg-raw-' + safeKey + '-a">—</span>)</span>'
            + '<span style="display:flex;gap:3px;margin-left:8px;">'
            + '<button data-espside-key="' + key + '" data-espside="both" '
            + 'style="' + btnBase + (sf === 'both' ? activeStyle : inactiveStyle) + '">Both</button>'
            + '<button data-espside-key="' + key + '" data-espside="home" '
            + 'style="' + btnBase + (sf === 'home' ? activeStyle : inactiveStyle) + '">' + g.game.home + '</button>'
            + '<button data-espside-key="' + key + '" data-espside="away" '
            + 'style="' + btnBase + (sf === 'away' ? activeStyle : inactiveStyle) + '">' + g.game.away + '</button>'
            + '</span>'
            + '<span style="display:flex;align-items:center;gap:2px;margin-left:8px;">'
            + '<button data-espboost-key="' + key + '" data-espboost-delta="-1" '
            + 'style="' + btnBase + 'background:#333;color:#ef5350;border:1px solid #555;">-1</button>'
            + '<span id="esp-tg-boost-' + safeKey + '" style="color:#fff;min-width:36px;text-align:center;font-size:11px;">'
            + (g.boost > 0 ? '+' + g.boost : g.boost) + '%</span>'
            + '<button data-espboost-key="' + key + '" data-espboost-delta="1" '
            + 'style="' + btnBase + 'background:#333;color:#4caf50;border:1px solid #555;">+1</button>'
            + '</span>'
            + '<button data-espremove-key="' + key + '" '
            + 'style="margin-left:auto;background:#333;border:1px solid #555;color:#ef5350;padding:2px 8px;border-radius:3px;cursor:pointer;font-size:11px;">&#10005;</button>'
            + '</div>';
    }
    el.innerHTML = html;
    el.querySelectorAll('[data-espside-key]').forEach(function(btn) {
        btn.addEventListener('click', function() {
            espSetSideFilter(btn.dataset.espsideKey, btn.dataset.espside);
        });
    });
    el.querySelectorAll('[data-espboost-key]').forEach(function(btn) {
        btn.addEventListener('click', function() {
            espAdjustBoost(btn.dataset.espboostKey, parseInt(btn.dataset.espboostDelta));
        });
    });
}

function espAdjustBoost(key, delta) {
    const g = espTrackedGames[key];
    if (!g) return;
    g.boost = Math.max(-20, Math.min(20, (g.boost || 0) + delta));
    const safeKey = key.replace(/[|\\]/g, '_');
    const el = document.getElementById('esp-tg-boost-' + safeKey);
    if (el) {
        el.textContent = (g.boost > 0 ? '+' + g.boost : g.boost) + '%';
        el.style.color = g.boost > 0 ? '#4caf50' : g.boost < 0 ? '#ef5350' : '#fff';
    }
    espLog('Boost ' + key + ' home ' + (g.boost > 0 ? '+' : '') + g.boost + '%');
}

function espSetSideFilter(key, side) {
    if (espTrackedGames[key]) {
        espTrackedGames[key].sideFilter = side;
        espRenderTrackedGames();
    }
}

function espLog(msg) {
    const el = document.getElementById('esp-trade-log');
    if (!el) return;
    const now = new Date();
    const ts = now.toLocaleTimeString('en-US', {hour12: false}) + '.' + String(now.getMilliseconds()).padStart(3, '0');
    el.innerHTML = '<div><span style="color:#555;">' + ts + '</span> ' + msg + '</div>' + el.innerHTML;
    if (el.children.length > 80) el.removeChild(el.lastChild);
}

function espToggleMode() {
    espTradeMode = espTradeMode === 'ioc' ? 'maker' : 'ioc';
    const btn = document.getElementById('esp-mode-btn');
    if (espTradeMode === 'maker') {
        btn.textContent = 'MAKER'; btn.style.background = '#1a237e'; btn.style.color = '#7986cb'; btn.style.borderColor = '#7986cb';
    } else {
        btn.textContent = 'IOC'; btn.style.background = '#333'; btn.style.color = '#ffa726'; btn.style.borderColor = '#ffa726';
    }
}

function espToggleTrade() {
    if (espTrading) espStopTrade(); else espStartTrade();
}

function espStartTrade() {
    espTrading = true;
    for (const g of Object.values(espTrackedGames)) { g.lastHomeOdds = null; g.lastAwayOdds = null; g.baseHomeLabel = null; g.baseAwayLabel = null; g.tradeBusy = false; g.locked = false; if (g.makerTimer) { clearTimeout(g.makerTimer); g.makerTimer = null; } g.pendingMakerFair = null; }
    const btn = document.getElementById('esp-trade-btn');
    btn.textContent = 'TRADE OFF'; btn.style.background = '#b71c1c'; btn.style.color = '#ff5252'; btn.style.borderColor = '#ff5252';
    const log = document.getElementById('esp-trade-log');
    if (log) { log.style.display = ''; log.innerHTML = ''; }
    espLog('<span style="color:#66bb6a;">TRADING ON</span> mode=' + espTradeMode);
    if (!espTradePoll) {
        fetch('/api/st_start', {method: 'POST'});
        espTradePoll = setInterval(espPollAll, 60);
    }
}

async function espStopTrade() {
    espTrading = false;
    const btn = document.getElementById('esp-trade-btn');
    btn.textContent = 'TRADE ON'; btn.style.background = '#1b5e20'; btn.style.color = '#66bb6a'; btn.style.borderColor = '#66bb6a';
    espLog('<span style="color:#ff5252;">TRADING OFF</span>');
    try {
        const promises = [];
        for (const g of Object.values(espTrackedGames)) {
            if (g.makerTimer) { clearTimeout(g.makerTimer); g.makerTimer = null; }
            g.pendingMakerFair = null;
            const gm = g.game;
            if (gm.tickers && gm.tickers.length) {
                promises.push(fetch('/api/cancel_all', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({tickers: gm.tickers}),
                }));
            }
            if (gm.polySeriesTickers && gm.polySeriesTickers.length) {
                const tokenIds = [];
                for (const t of gm.polySeriesTickers) { tokenIds.push(t.token_a); tokenIds.push(t.token_b); }
                promises.push(fetch('/api/poly_cancel_all', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({token_ids: tokenIds}),
                }));
            }
        }
        await Promise.all(promises);
    } catch(e) {}
}

// ── Poll loop ──
async function espPollAll() {
    const entries = Object.entries(espTrackedGames);
    if (entries.length === 0) return;

    if (espTrading) {
        fetch('/api/ws_status').then(r => r.json()).then(ws => {
            const el = document.getElementById('esp-ws-status');
            if (el) {
                el.textContent = ws.connected ? 'WS:ON' : 'WS:OFF';
                el.style.color = ws.connected ? '#66bb6a' : '#ef5350';
            }
            if (ws.fills && ws.fills.length > espLastWsFillCount) {
                const newFills = ws.fills.slice(espLastWsFillCount);
                espLastWsFillCount = ws.fills.length;
                for (const f of newFills) {
                    const tag = f.is_taker ? 'TAKER' : 'MAKER';
                    espLog('<span style="color:#ff9800;">[WS FILL] ' + tag + ' ' + f.side + ' ' + f.count + '@' + f.price + ' ' + f.ticker.split('-').pop() + '</span>');
                    playFillSound();
                }
            }
        }).catch(() => {});
    }

    try {
        const resp = await fetch('/api/st_state');
        const state = await resp.json();
        const odds = state.odds || {};
        const labels = state.labels || {};

        if (espTrading) {
            for (const [key, g] of entries) {
                const hl = labels[key + '_home_label'];
                const al = labels[key + '_away_label'];
                if (hl != null && g.baseHomeLabel == null) { g.baseHomeLabel = hl; espLog('Label baseline set: <span style="color:#ffa726;">' + hl + '</span>'); }
                if (al != null && g.baseAwayLabel == null) { g.baseAwayLabel = al; espLog('Label baseline set: <span style="color:#ffa726;">' + al + '</span>'); }
                if (g.baseHomeLabel != null && hl != null && hl !== g.baseHomeLabel) {
                    espLog('<span style="color:#ff5252;">LABEL SHIFT: "' + hl + '" != "' + g.baseHomeLabel + '" — STOPPING</span>');
                    espStopTrade(); playShiftAlarm(); return;
                }
                if (g.baseAwayLabel != null && al != null && al !== g.baseAwayLabel) {
                    espLog('<span style="color:#ff5252;">LABEL SHIFT: "' + al + '" != "' + g.baseAwayLabel + '" — STOPPING</span>');
                    espStopTrade(); playShiftAlarm(); return;
                }
            }
        }

        for (const [key, g] of entries) {
            const ho = odds[key + '_home'];
            const ao = odds[key + '_away'];
            const safeKey = key.replace(/[|\\]/g, '_');

            if ((ho == null || ao == null) && espTrading && g.lastHomeOdds != null) {
                const locked = ho == null && ao == null ? 'BOTH' : (ho == null ? 'HOME' : 'AWAY');
                if (!g.locked) {
                    espLog('<span style="color:#ff5252;font-weight:bold;">LOCKED (' + locked + ') — EMERGENCY CANCEL ' + key + '</span>');
                    g.locked = true;
                }
                if (g.makerTimer) { clearTimeout(g.makerTimer); g.makerTimer = null; }
                g.pendingMakerFair = null;
                g.tradeBusy = true;
                const gm = g.game;
                if (gm.tickers && gm.tickers.length) {
                    fetch('/api/cancel_all', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({tickers: gm.tickers}),
                    }).then(() => {
                        espLog('<span style="color:#ffa726;">CANCEL CONFIRMED for ' + key + '</span>');
                    }).catch(() => {}).finally(() => { g.tradeBusy = false; });
                } else {
                    g.tradeBusy = false;
                }
            }
            if (ho != null && ao != null) {
                if (g.locked) {
                    espLog('<span style="color:#66bb6a;font-weight:bold;">UNLOCKED ' + key + '</span> odds=' + ho.toFixed(2) + '/' + ao.toFixed(2));
                    g.locked = false;
                }
                const fair = powerDevig(ho, ao);
                const boost = g.boost || 0;
                const hf = Math.max(1, Math.min(99, Math.round(fair.home * 100) + boost));
                const af = 100 - hf;
                const rawH = document.getElementById('esp-tg-raw-' + safeKey + '-h');
                const rawA = document.getElementById('esp-tg-raw-' + safeKey + '-a');
                const fairH = document.getElementById('esp-tg-fair-' + safeKey + '-h');
                const fairA = document.getElementById('esp-tg-fair-' + safeKey + '-a');
                if (rawH) rawH.textContent = ho.toFixed(2);
                if (rawA) rawA.textContent = ao.toFixed(2);
                if (fairH) fairH.textContent = hf + 'c' + (boost ? ' (' + (boost > 0 ? '+' : '') + boost + ')' : '');
                if (fairA) fairA.textContent = af + 'c';

                if (espTrading && (ho !== g.lastHomeOdds || ao !== g.lastAwayOdds)) {
                    let tradeSide = 'both';
                    const contracts = parseInt(document.getElementById('esp-contracts').value);
                    const spread = parseInt(document.getElementById('esp-spread').value);
                    if (espTradeMode === 'ioc') {
                        if (g.lastHomeOdds != null && g.lastAwayOdds != null) {
                            const homeShortened = ho < g.lastHomeOdds;
                            const awayShortened = ao < g.lastAwayOdds;
                            if (homeShortened && !awayShortened) tradeSide = 'home';
                            else if (awayShortened && !homeShortened) tradeSide = 'away';
                            else if (!homeShortened && !awayShortened) tradeSide = 'none';
                            espLog('Odds moved ' + ho.toFixed(2) + '/' + ao.toFixed(2)
                                + (homeShortened ? ' <span style="color:#4caf50;">H&darr;</span>' : '')
                                + (awayShortened ? ' <span style="color:#ef5350;">A&darr;</span>' : '')
                                + ' &rarr; ' + tradeSide);
                        } else {
                            tradeSide = 'none';
                            espLog('Baseline set ' + ho.toFixed(2) + '/' + ao.toFixed(2) + ' fair=' + hf + '/' + af);
                        }
                    } else {
                        espLog('Odds moved ' + ho.toFixed(2) + '/' + ao.toFixed(2) + ' fair=' + hf + '/' + af + ' &rarr; reprice');
                    }
                    g.lastHomeOdds = ho; g.lastAwayOdds = ao;
                    const sf = g.sideFilter || 'both';
                    if (sf !== 'both') {
                        if (tradeSide === 'both') tradeSide = sf;
                        else if (tradeSide !== sf) tradeSide = 'none';
                    }
                    if (tradeSide !== 'none' && espTradeMode === 'maker') {
                        const gm = g.game;
                        if (gm.tickers && gm.tickers.length) {
                            fetch('/api/cancel_all', {
                                method: 'POST', headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({tickers: gm.tickers}),
                            }).catch(() => {});
                            espLog('<span style="color:#ffb74d;">CANCELED — reposting</span>');
                        }
                        if (g.makerTimer) clearTimeout(g.makerTimer);
                        g.pendingMakerFair = {hf, af, tradeSide};
                        g.makerTimer = setTimeout(() => {
                            g.makerTimer = null;
                            const pm = g.pendingMakerFair;
                            if (!pm || g.tradeBusy) return;
                            g.tradeBusy = true;
                            espLog('<span style="color:#e040fb;">POSTING MAKER</span> side=' + pm.tradeSide + ' fair=' + pm.hf + '/' + pm.af);
                            fetch('/api/screen_trade', {
                                method: 'POST', headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({
                                    home: gm.home, away: gm.away,
                                    tickers: gm.tickers,
                                    poly_tickers: gm.polySeriesTickers,
                                    contracts: contracts, spread_cents: spread,
                                    game_key: key,
                                    trade_side: pm.tradeSide,
                                    mode: 'maker',
                                    home_fair_cents: pm.hf,
                                }),
                            }).then(r => r.json()).then(d => {
                                if (d.orders && d.orders.length) {
                                    const fills = d.orders.filter(o => o.status === 'filled' || (o.filled && o.filled > 0));
                                    if (fills.length) {
                                        playFillSound();
                                        fills.forEach(o => espLog('<span style="color:#66bb6a;">FILLED</span> ' + o.team + ' @' + o.price + 'c'));
                                    }
                                }
                            }).catch(e => { espLog('<span style="color:#ff5252;">ERROR: ' + e + '</span>'); }).finally(() => { g.tradeBusy = false; });
                        }, 300);
                    } else if (tradeSide !== 'none' && !g.tradeBusy) {
                        g.tradeBusy = true;
                        const gm = g.game;
                        espLog('<span style="color:#e040fb;">SENDING ' + espTradeMode.toUpperCase() + '</span> side=' + tradeSide + ' fair=' + hf + '/' + af);
                        fetch('/api/screen_trade', {
                            method: 'POST', headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                home: gm.home, away: gm.away,
                                tickers: gm.tickers,
                                poly_tickers: gm.polySeriesTickers,
                                contracts: contracts, spread_cents: spread,
                                game_key: key,
                                trade_side: tradeSide,
                                mode: espTradeMode,
                                home_fair_cents: hf,
                            }),
                        }).then(r => r.json()).then(d => {
                            if (d.orders && d.orders.length) {
                                const fills = d.orders.filter(o => o.status === 'filled' || (o.filled && o.filled > 0));
                                if (fills.length) {
                                    playFillSound();
                                    fills.forEach(o => espLog('<span style="color:#66bb6a;">FILLED</span> ' + o.team + ' @' + o.price + 'c'));
                                }
                            }
                        }).catch(e => { espLog('<span style="color:#ff5252;">ERROR: ' + e + '</span>'); }).finally(() => { g.tradeBusy = false; });
                    } else if (tradeSide === 'none') {
                        espLog('<span style="color:#555;">no trade (side=none)</span>');
                    } else if (g.tradeBusy) {
                        espLog('<span style="color:#555;">skipped (busy)</span>');
                    }
                }
            }
        }
    } catch(e) {}
}
</script>
</div><!-- /container -->
</body>
</html>
"""


# ── Flask Routes ─────────────────────────────────────────────────────

@app.route('/')
def index():
    tickers = get_kalshi_cs2_tickers()

    poly_lookup = {}
    try:
        our_teams = list({t for key in tickers for t in key})
        poly_markets = fetch_poly_cs2_markets(pregame_only=False, today_only=False, our_teams=our_teams)
        for m in poly_markets:
            if m.get('home_team') and m.get('away_team'):
                pk = (m['home_team'], m['away_team'])
                poly_lookup.setdefault(pk, []).append({
                    'token_a': m['token_a'], 'token_b': m['token_b'],
                    'team_a': m['team_a'], 'team_b': m['team_b'],
                    'home_team': m['home_team'], 'away_team': m['away_team'],
                    'tick_size': m['tick_size'], 'neg_risk': m['neg_risk'],
                    'price_a': m['price_a'], 'price_b': m['price_b'],
                    'title': m['title'], 'platform': 'poly',
                })
        print(f"  [Poly] Found {sum(len(v) for v in poly_lookup.values())} series markets for {len(poly_lookup)} matchups")
    except Exception as e:
        print(f"  [Live Trader] Poly market fetch error: {e}")

    games = []
    for key, data in tickers.items():
        home, away = key
        poly_series = poly_lookup.get((home, away), []) or poly_lookup.get((away, home), [])
        games.append({
            'home': home,
            'away': away,
            'home_prob': data['home_prob'],
            'home_pct': f"{data['home_prob']:.0%}",
            'tickers_json': json.dumps(data['tickers']),
            'ou_tickers_json': json.dumps(data.get('ou_tickers', [])),
            'poly_series_json': json.dumps(poly_series),
        })
    return render_template_string(HTML_TEMPLATE, games=games, dry_run=DRY_RUN,
                                  contracts=CONTRACTS, spread=SPREAD_CENTS)


def _cancel_poly_tokens(poly_tickers):
    if not POLY_CLIENT:
        return
    for t in poly_tickers:
        try:
            POLY_CLIENT.cancel_token_orders(t.get('token_a', ''))
            POLY_CLIENT.cancel_token_orders(t.get('token_b', ''))
        except Exception:
            pass


def _post_poly_live_orders(poly_tickers, home_fair_cents, home, away,
                           contracts, spread_cents, ioc=False, trade_side='both'):
    orders = []
    away_fair_cents = 100 - home_fair_cents
    home_spread = _shrink_spread(spread_cents, home_fair_cents)
    away_spread = _shrink_spread(spread_cents, away_fair_cents)
    home_spread_bid = home_fair_cents - home_spread
    away_spread_bid = away_fair_cents - away_spread
    skip_home = home_spread_bid < 1
    skip_away = away_spread_bid < 1
    if trade_side == 'home':
        skip_away = True
    elif trade_side == 'away':
        skip_home = True

    print(f"[POLY {'IOC' if ioc else 'POST'}] fair={home_fair_cents}c home_spread={home_spread}c away_spread={away_spread}c (base={spread_cents}c) "
          f"-> home_bid={home_spread_bid}c{'(SKIP)' if skip_home else ''} "
          f"away_bid={away_spread_bid}c{'(SKIP)' if skip_away else ''} "
          f"contracts={contracts} shares")

    for t in poly_tickers:
        token_a = t.get('token_a', '')
        token_b = t.get('token_b', '')
        team_a = t.get('team_a', '')
        team_b = t.get('team_b', '')
        tick_size = t.get('tick_size', '0.01')
        neg_risk = t.get('neg_risk', False)
        poly_home = t.get('home_team', team_a)
        poly_away = t.get('away_team', team_b)

        # Match model's home team to the correct poly token.
        # token_a corresponds to team_a, token_b to team_b.
        home_lower = home.lower()
        away_lower = away.lower()
        ph = (poly_home or '').lower()
        pa = (poly_away or '').lower()
        if ph == home_lower or pa == away_lower:
            is_model_home_a = True
        elif ph == away_lower or pa == home_lower:
            is_model_home_a = False
        elif team_a.lower() == home_lower or team_b.lower() == away_lower:
            is_model_home_a = True
        elif team_b.lower() == home_lower or team_a.lower() == away_lower:
            is_model_home_a = False
        else:
            is_model_home_a = (poly_home == team_a)

        token_model_home = token_a if is_model_home_a else token_b
        token_model_away = token_b if is_model_home_a else token_a
        name_model_home = team_a if is_model_home_a else team_b
        name_model_away = team_b if is_model_home_a else team_a

        print(f"[POLY] Token mapping: model_home={home} -> poly={name_model_home} (token_{'a' if is_model_home_a else 'b'}) | "
              f"model_away={away} -> poly={name_model_away}")

        with ThreadPoolExecutor(max_workers=2) as ob_pool:
            fh = ob_pool.submit(get_poly_orderbook, token_model_home)
            fa = ob_pool.submit(get_poly_orderbook, token_model_away)
            ob_h = fh.result()
            ob_a = fa.result()
        best_bid_h = poly_best_bid(ob_h)
        best_bid_a = poly_best_bid(ob_a)
        ask_h_c = int(round(poly_best_ask(ob_h) * 100))
        ask_a_c = int(round(poly_best_ask(ob_a) * 100))

        # Home side
        hb = max(1, home_spread_bid)
        if not ioc:
            if best_bid_h > 0:
                hb = min(hb, int(round(best_bid_h * 100)) + 1)
            if hb >= ask_h_c > 0:
                hb = ask_h_c - 1
        if hb > home_fair_cents:
            hb = home_fair_cents

        # Away side
        ab = max(1, away_spread_bid)
        if not ioc:
            if best_bid_a > 0:
                ab = min(ab, int(round(best_bid_a * 100)) + 1)
            if ab >= ask_a_c > 0:
                ab = ask_a_c - 1
        if ab > away_fair_cents:
            ab = away_fair_cents

        print(f"[POLY] {name_model_home} (model home): fair={home_fair_cents}c bid={hb}c ask={ask_h_c}c skip={skip_home}")
        print(f"[POLY] {name_model_away} (model away): fair={away_fair_cents}c bid={ab}c ask={ask_a_c}c skip={skip_away}")

        def _poly_side(team_name, token_id, bid_cents, fair_cents, skip_flag, best_ask_c):
            if skip_flag or not (1 <= bid_cents <= 99):
                return None
            if DRY_RUN or not POLY_CLIENT:
                return {'team': f'{team_name} YES (Poly)', 'price': bid_cents,
                        'contracts': contracts, 'status': 'dry-run'}
            if ioc:
                if best_ask_c > bid_cents:
                    print(f"[POLY] IOC SKIP {team_name}: ask={best_ask_c}c > bid={bid_cents}c")
                    return None
                take_price = round(best_ask_c / 100.0, 2)
                print(f"[POLY] IOC TAKE {team_name} {contracts} shares @ {take_price} "
                      f"(ask={best_ask_c}c bid={bid_cents}c fair={fair_cents}c)")
                resp = POLY_CLIENT.place_order(
                    token_id, "BUY", take_price, contracts,
                    tick_size=tick_size, neg_risk=neg_risk, order_type="GTC")
                return {'team': f'{team_name} YES (Poly)', 'price': best_ask_c,
                        'contracts': contracts,
                        'status': 'filled' if resp else 'failed'}
            else:
                POLY_CLIENT.cancel_token_orders(token_id)
                price = round(bid_cents / 100.0, 2)
                print(f"[POLY] GTC BUY {team_name} {contracts} shares @ {price}")
                resp = POLY_CLIENT.place_order(
                    token_id, "BUY", price, contracts,
                    tick_size=tick_size, neg_risk=neg_risk, order_type="GTC")
                with POLY_TOKEN_LOCK:
                    POLY_TOKEN_IDS.append(token_id)
                return {'team': f'{team_name} YES (Poly)', 'price': bid_cents,
                        'contracts': contracts,
                        'status': 'placed' if resp else 'failed'}

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = []
            futures.append(pool.submit(_poly_side, name_model_home, token_model_home,
                                       hb, home_fair_cents, skip_home, ask_h_c))
            futures.append(pool.submit(_poly_side, name_model_away, token_model_away,
                                       ab, away_fair_cents, skip_away, ask_a_c))
            for f in as_completed(futures):
                result = f.result()
                if result:
                    orders.append(result)

    if orders:
        print(f"[POLY] Posted {len(orders)} orders: " +
              ", ".join(f"{o['team']} @ {o['price']}c ({o['status']})" for o in orders))
    return orders


def _compute_and_post(data, cancel_first=False, post_orders_flag=True, ioc=False):
    home = data['home']
    away = data['away']
    home_prob = data['home_prob']
    home_maps = data.get('home_maps', 0)
    away_maps = data.get('away_maps', 0)
    maps_played = data.get('maps_played', 0)
    home_rounds = data.get('home_rounds', 0)
    away_rounds = data.get('away_rounds', 0)
    contracts = data.get('contracts', CONTRACTS)
    spread_cents = data.get('spread_cents', SPREAD_CENTS)
    tickers = data.get('tickers', [])

    best_of = data.get('best_of', 3)
    home_is_ct = data.get('home_is_ct')
    ct_win_pct = data.get('ct_win_pct')
    t_win_pct = data.get('t_win_pct')
    manual_home_buy = data.get('home_buy')
    manual_away_buy = data.get('away_buy')
    manual_home_money = data.get('home_money')
    manual_away_money = data.get('away_money')
    manual_home_alive = data.get('home_alive', 5)
    manual_away_alive = data.get('away_alive', 5)

    map_p = series_to_map_prob(home_prob)
    neutral_map_p = map_p
    ct_round_rate = ct_win_pct / 100.0 if ct_win_pct is not None else None

    per_map = [neutral_map_p] * best_of
    map_prob = map_p
    if maps_played < best_of:
        if ct_round_rate is not None and home_is_ct is not None:
            map_prob = live_round_win_prob(home_rounds, away_rounds, neutral_map_p,
                                           home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
        elif home_rounds > 0 or away_rounds > 0:
            map_prob = live_round_win_prob(home_rounds, away_rounds, neutral_map_p,
                                           home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
        per_map[maps_played] = map_prob

    # Economy adjustment from scoreboard
    econ_info = {}
    original_home_is_ct = home_is_ct
    sb_state = HLTV_SCRAPER.get_state() if HLTV_SCRAPER else None
    if sb_state and (sb_state.get('ct_players') or sb_state.get('t_players')):
        ct_players = sb_state.get('ct_players', [])
        t_players = sb_state.get('t_players', [])

        # Figure out which side home team is on from scoreboard team names
        ct_name = sb_state.get('ct_team', '').lower()
        t_name = sb_state.get('t_team', '').lower()
        home_lower = home.lower()
        away_lower = away.lower()
        sb_home_is_ct = None
        if ct_name and (home_lower in ct_name or ct_name in home_lower):
            sb_home_is_ct = True
        elif t_name and (home_lower in t_name or t_name in home_lower):
            sb_home_is_ct = False
        elif t_name and (away_lower in t_name or t_name in away_lower):
            sb_home_is_ct = True
        elif ct_name and (away_lower in ct_name or ct_name in away_lower):
            sb_home_is_ct = False

        if sb_home_is_ct is not None:
            home_is_ct = sb_home_is_ct
            bomb_planted = sb_state.get('bomb_planted', False)
            timer_secs = sb_state.get('timer_seconds', -1)
            bomb_src = sb_state.get('bomb_source', 'css')
            bomb_debug = sb_state.get('_bomb_debug')
            if bomb_debug and not bomb_planted:
                print(f"[BOMB-DBG] Not planted. Elements: {bomb_debug[:3]}")
            elif bomb_planted:
                print(f"[BOMB] PLANTED ({bomb_src}) | timer={timer_secs}s")
            rh = sb_state.get('round_history', [])
            econ_map_p, econ_detail = economy_adjusted_map_prob(
                ct_players, t_players, neutral_map_p, home_is_ct, home_rounds, away_rounds,
                bomb_planted=bomb_planted, timer_seconds=timer_secs,
                round_history=rh, ct_round_rate=ct_round_rate)

            if econ_detail:
                map_idx = maps_played if maps_played < best_of else best_of - 1
                per_map[map_idx] = econ_map_p
                map_prob = econ_map_p
                econ_info = econ_detail

                phase_tag = {"round_over": " [ROUND OVER]", "buy_phase": " [BUY PHASE]"}.get(econ_detail.get('phase', ''), "")
                print(f"[ECON] {home} [{econ_detail['home_buy'].upper()}] "
                      f"${econ_detail['home_avg_money']} | "
                      f"{away} [{econ_detail['away_buy'].upper()}] "
                      f"${econ_detail['away_avg_money']} | "
                      f"round_p={econ_detail['home_round_p']:.3f} "
                      f"econ_map={econ_map_p:.3f} "
                      f"w={econ_detail['econ_weight']:.2f}{phase_tag}")
                if econ_detail.get('home_buy_if_win'):
                    print(f"[FWD]  WIN  -> {home} {econ_detail['home_buy_if_win'].upper()} ${econ_detail['home_money_if_win']} "
                          f"vs {away} {econ_detail['away_buy_if_win'].upper()} ${econ_detail['away_money_if_win']} "
                          f"-> map {econ_detail['map_prob_if_win']:.1%}")
                    print(f"[FWD]  LOSE -> {home} {econ_detail['home_buy_if_lose'].upper()} ${econ_detail['home_money_if_lose']} "
                          f"vs {away} {econ_detail['away_buy_if_lose'].upper()} ${econ_detail['away_money_if_lose']} "
                          f"-> map {econ_detail['map_prob_if_lose']:.1%}")

    # Manual buy/alive override — apply when no HLTV scoreboard
    has_manual_buy = (manual_home_buy or manual_away_buy) and home_is_ct is not None
    has_manual_alive = (manual_home_alive != 5 or manual_away_alive != 5) and not econ_info

    if has_manual_buy or has_manual_alive:
        eff_home_buy = manual_home_buy or 'full'
        eff_away_buy = manual_away_buy or 'full'

        # Start with skill-based round probability
        if home_is_ct is not None:
            skill_p = neutral_map_p if home_is_ct else (1 - neutral_map_p)
        else:
            skill_p = map_prob

        home_round_p = skill_p

        # Apply buy type adjustment
        if has_manual_buy:
            ct_buy = eff_home_buy if home_is_ct else eff_away_buy
            t_buy = eff_away_buy if home_is_ct else eff_home_buy
            matchup_rate = BUY_MATCHUP_RATES.get((ct_buy, t_buy), 0.50)
            econ_weight = 0.55 + abs(matchup_rate - 0.50) * 1.2
            econ_weight = min(econ_weight, 0.92)
            ct_round_p = (1 - econ_weight) * skill_p + econ_weight * matchup_rate
            home_round_p = ct_round_p if home_is_ct else (1 - ct_round_p)

        # Apply alive adjustment on top
        if has_manual_alive and home_is_ct is not None:
            ct_a = manual_home_alive if home_is_ct else manual_away_alive
            t_a = manual_away_alive if home_is_ct else manual_home_alive
            alive_rate = ALIVE_ADVANTAGE.get((ct_a, t_a), 0.50)
            # Blend alive advantage with current round probability
            alive_shift = alive_rate - 0.50
            ct_current = home_round_p if home_is_ct else (1 - home_round_p)
            ct_adjusted = max(0.01, min(0.99, ct_current + alive_shift))
            home_round_p = ct_adjusted if home_is_ct else (1 - ct_adjusted)
        elif has_manual_alive:
            alive_rate = ALIVE_ADVANTAGE.get((manual_home_alive, manual_away_alive), 0.50)
            alive_shift = alive_rate - 0.50
            home_round_p = max(0.01, min(0.99, home_round_p + alive_shift))

        p_map_if_win = live_round_win_prob(home_rounds + 1, away_rounds, neutral_map_p,
                                            home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
        p_map_if_lose = live_round_win_prob(home_rounds, away_rounds + 1, neutral_map_p,
                                             home_is_ct=home_is_ct, ct_round_rate=ct_round_rate)
        econ_map_p = home_round_p * p_map_if_win + (1 - home_round_p) * p_map_if_lose
        map_idx = maps_played if maps_played < best_of else best_of - 1
        per_map[map_idx] = econ_map_p
        map_prob = econ_map_p
        econ_info = {
            'home_buy': eff_home_buy if has_manual_buy else '?',
            'away_buy': eff_away_buy if has_manual_buy else '?',
            'home_avg_money': round(manual_home_money / 5) if manual_home_money else 0,
            'away_avg_money': round(manual_away_money / 5) if manual_away_money else 0,
            'home_avg_power': 0, 'away_avg_power': 0,
            'home_alive': manual_home_alive, 'away_alive': manual_away_alive,
            'home_is_ct': home_is_ct, 'phase': 'manual',
            'bomb_planted': False, 'bomb_status': 'none',
            'timer_seconds': -1,
            'econ_weight': round(econ_weight, 2) if has_manual_buy else 0,
            'home_round_p': round(home_round_p, 4),
            'econ_map_prob': round(econ_map_p, 4),
        }
        buy_tag = f"[{eff_home_buy.upper()}] vs [{eff_away_buy.upper()}]" if has_manual_buy else ""
        alive_tag = f"{manual_home_alive}v{manual_away_alive}" if has_manual_alive else "5v5"
        print(f"[MANUAL] {home} {buy_tag} {alive_tag} {away} | "
              f"round_p={home_round_p:.3f} map={econ_map_p:.3f}")

    if best_of == 1:
        live_wp = per_map[0]
    elif best_of == 5:
        live_wp = live_bo5_win_prob(home_maps, away_maps, maps_played, per_map)
    else:
        live_wp = live_bo3_win_prob(home_maps, away_maps, maps_played, per_map)
    home_fair = max(1, min(99, int(round(live_wp * 100))))
    away_fair = 100 - home_fair

    print(f"[COMPUTE] {away} vs {home} | BO{best_of} maps={home_maps}-{away_maps} "
          f"rounds={home_rounds}-{away_rounds} | pregame={home_prob:.3f} "
          f"map={map_prob:.3f} live={live_wp:.3f} -> {home_fair}c")

    alt = compute_alt_lines_bo3(home_maps, away_maps, maps_played, per_map) if best_of == 3 else {}

    result = {
        'home': home, 'away': away,
        'home_fair': home_fair, 'away_fair': away_fair,
        'live_prob': round(live_wp, 4),
        'pregame_prob': round(home_prob, 4),
        'map_prob': round(map_prob, 4),
        'home_maps': home_maps, 'away_maps': away_maps,
        'home_rounds': home_rounds, 'away_rounds': away_rounds,
        'alt_lines': alt,
        'econ': econ_info,
        'cancelled': False,
    }

    poly_tickers = data.get('poly_tickers', [])

    if KALSHI_WS and tickers:
        KALSHI_WS.subscribe([t['ticker'] for t in tickers if 'ticker' in t])

    if cancel_first:
        ticker_names = [t['ticker'] for t in tickers if 'ticker' in t]
        cancel_all_live_orders(tickers=ticker_names)
        if poly_tickers:
            _cancel_poly_tokens(poly_tickers)
        result['cancelled'] = True

    if post_orders_flag and tickers:
        orders = post_live_orders(tickers, home_fair, contracts, spread_cents,
                                  home_name=home, away_name=away)
        result['orders'] = orders
    elif ioc and tickers:
        orders = post_ioc_orders(tickers, home_fair, contracts, spread_cents,
                                 home_name=home, away_name=away)
        result['orders'] = orders

    if poly_tickers and (post_orders_flag or ioc):
        poly_orders = _post_poly_live_orders(
            poly_tickers, home_fair, home, away, contracts, spread_cents, ioc=ioc)
        result['orders'] = result.get('orders', []) + poly_orders

    return result


@app.route('/api/compute_fair', methods=['POST'])
def api_compute_fair():
    return jsonify(_compute_and_post(request.json, post_orders_flag=False))


@app.route('/api/compute_and_post', methods=['POST'])
def api_compute_and_post():
    return jsonify(_compute_and_post(request.json))


@app.route('/api/compute_and_ioc', methods=['POST'])
def api_compute_and_ioc():
    return jsonify(_compute_and_post(request.json, post_orders_flag=False, ioc=True))


@app.route('/api/cancel_and_repost', methods=['POST'])
def api_cancel_and_repost():
    return jsonify(_compute_and_post(request.json, cancel_first=True))


@app.route('/api/auto_trade', methods=['POST'])
def api_auto_trade():
    """Cancel existing, compute fair, post at min(fair - spread, best_bid) per side."""
    data = request.json
    result = _compute_and_post(data, cancel_first=True, post_orders_flag=False)

    tickers = data.get('tickers', [])
    contracts = data.get('contracts', CONTRACTS)
    spread_cents = data.get('spread_cents', SPREAD_CENTS)
    home_fair = result['home_fair']
    away_fair = result['away_fair']
    home = result['home']
    away = result['away']

    home_target = max(1, home_fair - spread_cents)
    away_target = max(1, away_fair - spread_cents)

    orders_to_place = []
    for t in tickers:
        ticker = t['ticker']
        home_is_yes = t['home_is_yes']
        yes_target = home_target if home_is_yes else away_target
        no_target = away_target if home_is_yes else home_target
        yes_team = home if home_is_yes else away
        no_team = away if home_is_yes else home
        yes_fair = home_fair if home_is_yes else away_fair
        no_fair = away_fair if home_is_yes else home_fair

        ob = _fetch_orderbook_prices(ticker)
        if ob:
            # Cap at best bid — never go above it
            if ob['yes_best_bid'] is not None:
                yes_target = min(yes_target, ob['yes_best_bid'])
            if ob['no_best_bid'] is not None:
                no_target = min(no_target, ob['no_best_bid'])
            # Also cap below best ask to stay maker
            if ob['yes_best_ask'] is not None:
                yes_target = min(yes_target, ob['yes_best_ask'] - 1)
            if ob['no_best_ask'] is not None:
                no_target = min(no_target, ob['no_best_ask'] - 1)

        if 1 <= yes_target <= 99 and yes_target < yes_fair:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_target,
                'team': f"{yes_team} YES", 'fair': yes_fair,
            })
        if 1 <= no_target <= 99 and no_target < no_fair:
            orders_to_place.append({
                'ticker': ticker, 'side': 'no', 'price': no_target,
                'team': f"{no_team} NO", 'fair': no_fair,
            })

    print(f"[AUTO-TRADE] {home} fair={home_fair}c target={home_target}c | "
          f"{away} fair={away_fair}c target={away_target}c | "
          f"{len(orders_to_place)} orders")

    results = []
    def _place(spec):
        ok = place_tracked_order(spec['ticker'], spec['side'], contracts, spec['price'])
        return {
            'team': spec['team'], 'ticker': spec['ticker'],
            'kalshi_side': spec['side'], 'fair': spec['fair'],
            'price': spec['price'], 'contracts': contracts,
            'status': 'placed' if ok else 'failed',
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(_place, o) for o in orders_to_place]):
            results.append(f.result())

    poly_tickers = data.get('poly_tickers', [])
    if poly_tickers:
        poly_orders = _post_poly_live_orders(
            poly_tickers, home_fair, home, away, contracts, spread_cents)
        results.extend(poly_orders)

    result['orders'] = results
    return jsonify(result)


def _compute_and_post_ou(data, cancel_first=False, post_orders_flag=True, ioc=False):
    home = data['home']
    away = data['away']
    home_prob = data['home_prob']
    home_maps = data.get('home_maps', 0)
    away_maps = data.get('away_maps', 0)
    maps_played = data.get('maps_played', 0)
    home_rounds = data.get('home_rounds', 0)
    away_rounds = data.get('away_rounds', 0)
    contracts = data.get('contracts', CONTRACTS)
    spread_cents = data.get('spread_cents', SPREAD_CENTS)
    ou_tickers = data.get('ou_tickers', [])

    map_p = series_to_map_prob(home_prob)
    ct_win_pct_ou = data.get('ct_win_pct')
    ct_round_rate_ou = ct_win_pct_ou / 100.0 if ct_win_pct_ou is not None else None
    home_is_ct_ou = data.get('home_is_ct')
    per_map = [map_p] * 3
    if maps_played < 3 and (home_rounds > 0 or away_rounds > 0):
        per_map[maps_played] = live_round_win_prob(home_rounds, away_rounds, map_p,
                                                    home_is_ct=home_is_ct_ou,
                                                    ct_round_rate=ct_round_rate_ou)

    sb_state = HLTV_SCRAPER.get_state() if HLTV_SCRAPER else None
    if sb_state and (sb_state.get('ct_players') or sb_state.get('t_players')):
        ct_name = sb_state.get('ct_team', '').lower()
        t_name = sb_state.get('t_team', '').lower()
        home_lower = home.lower()
        away_lower = away.lower()
        home_is_ct = None
        if ct_name and (home_lower in ct_name or ct_name in home_lower):
            home_is_ct = True
        elif t_name and (home_lower in t_name or t_name in home_lower):
            home_is_ct = False
        elif t_name and (away_lower in t_name or t_name in away_lower):
            home_is_ct = True
        elif ct_name and (away_lower in ct_name or ct_name in away_lower):
            home_is_ct = False

        if home_is_ct is not None:
            bomb_planted = sb_state.get('bomb_planted', False)
            timer_secs = sb_state.get('timer_seconds', -1)
            rh = sb_state.get('round_history', [])
            econ_map_p, econ_detail = economy_adjusted_map_prob(
                sb_state.get('ct_players', []), sb_state.get('t_players', []),
                map_p, home_is_ct, home_rounds, away_rounds,
                bomb_planted=bomb_planted, timer_seconds=timer_secs,
                round_history=rh, ct_round_rate=ct_round_rate_ou)
            if econ_detail:
                map_idx = maps_played if maps_played < 3 else 2
                per_map[map_idx] = econ_map_p

    alt = compute_alt_lines_bo3(home_maps, away_maps, maps_played, per_map)
    ou = alt.get('over_under', {})
    over_fair = ou.get('over_fair', 50)
    under_fair = ou.get('under_fair', 50)

    print(f"[OU-COMPUTE] {away} vs {home} | maps={home_maps}-{away_maps} "
          f"rounds={home_rounds}-{away_rounds} | over={over_fair}c under={under_fair}c")

    result = {
        'home': home, 'away': away,
        'over_fair': over_fair, 'under_fair': under_fair,
        'over_prob': ou.get('over_prob', 0.5),
        'under_prob': ou.get('under_prob', 0.5),
        'home_maps': home_maps, 'away_maps': away_maps,
        'cancelled': False,
    }

    if cancel_first:
        ticker_names = [t['ticker'] for t in ou_tickers if 'ticker' in t]
        cancel_all_live_orders(tickers=ticker_names)
        result['cancelled'] = True

    if post_orders_flag and ou_tickers:
        orders = post_ou_orders(ou_tickers, over_fair, contracts, spread_cents)
        result['orders'] = orders
    elif ioc and ou_tickers:
        orders = post_ou_ioc_orders(ou_tickers, over_fair, contracts, spread_cents)
        result['orders'] = orders

    return result


def post_ou_orders(tickers_list, over_fair_cents, contracts, spread_cents):
    under_fair_cents = 100 - over_fair_cents
    skip_over = (over_fair_cents - spread_cents) < 1
    skip_under = (under_fair_cents - spread_cents) < 1
    over_bid = over_fair_cents - spread_cents
    under_bid = under_fair_cents - spread_cents

    if over_bid > over_fair_cents:
        over_bid = over_fair_cents
    if under_bid > under_fair_cents:
        under_bid = under_fair_cents

    orders_to_place = []
    for t in tickers_list:
        ticker = t['ticker']

        ob = _fetch_orderbook_prices(ticker)
        yes_bid = over_bid
        no_bid = under_bid
        if ob:
            if ob['yes_best_ask'] is not None and yes_bid >= ob['yes_best_ask']:
                yes_bid = ob['yes_best_ask'] - 1
                print(f"[MAKER] Over YES: capped -> {yes_bid}c (ask={ob['yes_best_ask']}c)")
            if ob['no_best_ask'] is not None and no_bid >= ob['no_best_ask']:
                no_bid = ob['no_best_ask'] - 1
                print(f"[MAKER] Under NO: capped -> {no_bid}c (ask={ob['no_best_ask']}c)")

        if 1 <= yes_bid <= 99 and not skip_over:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_bid,
                'team': 'Over 2.5 YES', 'fair': over_fair_cents,
            })
        if 1 <= no_bid <= 99 and not skip_under:
            orders_to_place.append({
                'ticker': ticker, 'side': 'no', 'price': no_bid,
                'team': 'Under 2.5 NO', 'fair': under_fair_cents,
            })

    print(f"[OU-Post] Firing {len(orders_to_place)} orders | "
          f"Over fair={over_fair_cents}c bid={over_bid}c | "
          f"Under fair={under_fair_cents}c bid={under_bid}c")

    results = []
    def _place(spec):
        ok = place_tracked_order(spec['ticker'], spec['side'], contracts, spec['price'])
        return {
            'team': spec['team'], 'ticker': spec['ticker'],
            'kalshi_side': spec['side'], 'fair': spec['fair'],
            'price': spec['price'], 'contracts': contracts,
            'status': 'placed' if ok else 'failed',
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(_place, o) for o in orders_to_place]):
            results.append(f.result())
    return results


def post_ou_ioc_orders(tickers_list, over_fair_cents, contracts, spread_cents):
    under_fair_cents = 100 - over_fair_cents
    skip_over = (over_fair_cents - spread_cents) < 1
    skip_under = (under_fair_cents - spread_cents) < 1
    over_bid = max(1, over_fair_cents - spread_cents)
    under_bid = max(1, under_fair_cents - spread_cents)

    if over_bid > over_fair_cents:
        over_bid = over_fair_cents
    if under_bid > under_fair_cents:
        under_bid = under_fair_cents

    orders_to_place = []
    for t in tickers_list:
        ticker = t['ticker']
        if 1 <= over_bid <= 99 and not skip_over:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': over_bid,
                'team': 'Over 2.5 YES', 'fair': over_fair_cents,
            })
        if 1 <= under_bid <= 99 and not skip_under:
            orders_to_place.append({
                'ticker': ticker, 'side': 'no', 'price': under_bid,
                'team': 'Under 2.5 NO', 'fair': under_fair_cents,
            })

    results = []
    def _take(spec):
        filled, total = place_ioc_order(spec['ticker'], spec['side'],
                                         contracts, spec['price'])
        return {
            'team': spec['team'], 'ticker': spec['ticker'],
            'kalshi_side': spec['side'], 'fair': spec['fair'],
            'price': spec['price'], 'contracts': contracts,
            'filled': filled,
            'status': 'filled' if filled > 0 else 'not_filled',
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed([pool.submit(_take, o) for o in orders_to_place]):
            results.append(f.result())
    return results


@app.route('/api/ou_compute_and_post', methods=['POST'])
def api_ou_compute_and_post():
    return jsonify(_compute_and_post_ou(request.json))


@app.route('/api/ou_compute_and_ioc', methods=['POST'])
def api_ou_compute_and_ioc():
    return jsonify(_compute_and_post_ou(request.json, post_orders_flag=False, ioc=True))


@app.route('/api/ou_cancel_and_repost', methods=['POST'])
def api_ou_cancel_and_repost():
    return jsonify(_compute_and_post_ou(request.json, cancel_first=True))


@app.route('/api/cancel_all', methods=['POST'])
def api_cancel_all():
    data = request.json or {}
    tickers = [t['ticker'] for t in data.get('tickers', []) if 'ticker' in t]
    ok, msg = cancel_all_live_orders(tickers=tickers)
    return jsonify({'ok': ok, 'message': msg})


@app.route('/api/poly_cancel_all', methods=['POST'])
def api_poly_cancel_all():
    data = request.json
    token_ids = data.get('token_ids', [])
    if not token_ids or not POLY_CLIENT:
        msg = 'Poly: no client (dry run)' if not POLY_CLIENT else 'No tokens to cancel'
        return jsonify({'message': msg})
    cancelled = 0
    for tid in token_ids:
        if tid:
            try:
                POLY_CLIENT.cancel_token_orders(tid)
                cancelled += 1
            except Exception:
                pass
    with POLY_TOKEN_LOCK:
        for tid in token_ids:
            if tid in POLY_TOKEN_IDS:
                POLY_TOKEN_IDS.remove(tid)
    return jsonify({'message': f'Poly: cancelled {cancelled}/{len(token_ids)} markets'})


@app.route('/api/poly_orderbook', methods=['POST'])
def api_poly_orderbook():
    data = request.json
    poly_tickers = data.get('poly_tickers', [])
    results = []
    for t in poly_tickers:
        token_a = t.get('token_a', '')
        token_b = t.get('token_b', '')
        team_a = t.get('team_a', '?')
        team_b = t.get('team_b', '?')
        home_team = t.get('home_team', team_a)
        away_team = t.get('away_team', team_b)
        is_home_a = (home_team == team_a)
        token_home = token_a if is_home_a else token_b
        token_away = token_b if is_home_a else token_a
        for token, team, opp in [
            (token_home, home_team, away_team),
            (token_away, away_team, home_team),
        ]:
            try:
                ob = get_poly_orderbook(token)
                bids = [[int(round(float(b['price']) * 100)), int(float(b.get('size', 0)))]
                        for b in ob.get('bids', []) if float(b.get('price', 0)) > 0]
                asks = [[int(round(float(a['price']) * 100)), int(float(a.get('size', 0)))]
                        for a in ob.get('asks', []) if float(a.get('price', 0)) > 0]
                bids.sort(key=lambda x: -x[0])
                asks.sort(key=lambda x: x[0])
                results.append({
                    'ticker': f"Poly:{token[:8]}",
                    'yes_team': f'{team} (Poly)', 'no_team': f'{opp} (Poly)',
                    'yes_bids': bids, 'yes_asks': asks,
                    'my_bid_prices': [], 'my_ask_prices': [],
                })
            except Exception as exc:
                results.append({
                    'ticker': f"Poly:{token[:8]}",
                    'yes_team': f'{team} (Poly)', 'no_team': f'{opp} (Poly)',
                    'error': str(exc),
                })
    return jsonify({'books': results})


@app.route('/api/manual_order', methods=['POST'])
def api_manual_order():
    data = request.json
    ticker = data.get('ticker', '')
    side = data.get('side', 'yes')
    price = data.get('price', 50)
    contracts = data.get('contracts', CONTRACTS)
    ioc = data.get('ioc', False)

    if not ticker:
        return jsonify({'error': 'No ticker'})

    team_label = f"{side.upper()} @ {ticker}"
    if ioc:
        filled, _ = place_ioc_order(ticker, side, contracts, price)
        order = {
            'team': team_label, 'price': price, 'contracts': contracts,
            'filled': filled, 'status': 'filled' if filled > 0 else 'not_filled',
        }
    else:
        ok = place_tracked_order(ticker, side, contracts, price)
        order = {
            'team': team_label, 'price': price, 'contracts': contracts,
            'status': 'placed' if ok else 'failed',
        }

    order_type = "IOC" if ioc else "Limit"
    msg = f"{order_type}: {side.upper()} {contracts}x @ {price}c"
    return jsonify({'message': msg, 'orders': [order]})


@app.route('/api/jump_bid', methods=['POST'])
def api_jump_bid():
    data = request.json
    ticker = data.get('ticker', '')
    side = data.get('side', 'yes')
    contracts = data.get('contracts', CONTRACTS)

    if not ticker:
        return jsonify({'error': 'No ticker'})

    best_bid = _fetch_best_bid(ticker, side)
    if best_bid is None:
        return jsonify({'error': 'Could not fetch orderbook'})

    jump_price = min(best_bid + 1, 99)
    ok = place_tracked_order(ticker, side, contracts, jump_price)
    order = {
        'team': f"{side.upper()} @ {ticker}", 'best_bid': best_bid,
        'price': jump_price, 'contracts': contracts,
        'status': 'placed' if ok else 'failed',
    }
    msg = f"Jump bid: {side.upper()} @ {jump_price}c (best was {best_bid}c)"
    return jsonify({'message': msg, 'orders': [order]})


@app.route('/api/orderbook', methods=['POST'])
def api_orderbook():
    data = request.json
    tickers = data.get('tickers', [])

    with PLACED_ORDER_LOCK:
        resting = list(PLACED_ORDER_IDS)

    results = []
    for t in tickers:
        ticker = t.get('ticker', '')
        yes_team = t.get('yes_team', '?')
        no_team = t.get('no_team', '?')
        try:
            url = f"{KALSHI_BASE}/markets/{ticker}/orderbook"
            resp = http_requests.get(url, timeout=5)
            if resp.status_code != 200:
                results.append({'ticker': ticker, 'yes_team': yes_team,
                                'no_team': no_team, 'error': f'HTTP {resp.status_code}'})
                continue
            ob_json = resp.json()
            ob = ob_json.get('orderbook', {})
            yes_bids = ob.get('yes', [])
            no_bids = ob.get('no', [])
            if not yes_bids and not no_bids:
                ob_fp = ob_json.get('orderbook_fp', {})
                yes_fp = ob_fp.get('yes_dollars', [])
                no_fp = ob_fp.get('no_dollars', [])
                yes_bids = [[int(round(float(p) * 100)), int(float(q))]
                            for p, q in yes_fp]
                no_bids = [[int(round(float(p) * 100)), int(float(q))]
                           for p, q in no_fp]
            yes_bids.sort(key=lambda x: -x[0])
            no_bids.sort(key=lambda x: -x[0])
            yes_asks = [[100 - b[0], b[1]] for b in no_bids]
            yes_asks.sort(key=lambda x: x[0])

            my_bid = [r['yes_price'] for r in resting
                      if r['ticker'] == ticker and r['side'] == 'yes']
            my_ask = [r['yes_price'] for r in resting
                      if r['ticker'] == ticker and r['side'] == 'no']

            results.append({
                'ticker': ticker, 'yes_team': yes_team, 'no_team': no_team,
                'yes_bids': yes_bids, 'yes_asks': yes_asks,
                'my_bid_prices': my_bid, 'my_ask_prices': my_ask,
            })
        except Exception as exc:
            results.append({'ticker': ticker, 'yes_team': yes_team,
                            'no_team': no_team, 'error': str(exc)})
    return jsonify({'books': results})


# ── Forfeit Iceberg Route ─────────────────────────────────────────────

@app.route('/api/forfeit/ioc', methods=['POST'])
def api_forfeit_ioc():
    data = request.json
    ticker = data.get('ticker', '')
    side = data.get('side', 'yes')
    max_price = data.get('max_price', 97)
    count = data.get('count', 1)

    if not ticker:
        return jsonify({'error': 'No ticker'})

    ob = _fetch_orderbook_prices(ticker)
    if not ob:
        return jsonify({'filled': 0, 'no_ask': True})

    best_ask = ob.get(f'{side}_best_ask')
    if best_ask is None:
        return jsonify({'filled': 0, 'no_ask': True})

    if best_ask > max_price:
        return jsonify({'filled': 0, 'best_ask': best_ask, 'tried_price': max_price})

    buy_price = best_ask
    filled, total = place_ioc_order(ticker, side, count, buy_price)
    print(f"[FORFEIT IOC] {side.upper()} {filled}/{count} @ {buy_price}c (ask={best_ask}c)")
    return jsonify({
        'filled': filled, 'count': count,
        'fill_price': buy_price, 'best_ask': best_ask,
    })


# ── HLTV Scoreboard Routes ────────────────────────────────────────────

@app.route('/api/hltv_live')
def api_hltv_live():
    matches = fetch_hltv_live_matches()
    return jsonify({'matches': matches})


@app.route('/api/watch_match', methods=['POST'])
def api_watch_match():
    global HLTV_SCRAPER
    data = request.json or {}
    match_id = data.get('match_id', '')
    url = data.get('url', '')
    if not url:
        return jsonify({'error': 'No match URL'})
    if HLTV_SCRAPER is None:
        HLTV_SCRAPER = HLTVScoreboard()
    HLTV_SCRAPER.watch(match_id, url)
    return jsonify({'ok': True, 'match_id': match_id})


@app.route('/api/scoreboard')
def api_scoreboard():
    if HLTV_SCRAPER is None:
        return jsonify({'watching': False, 'state': None})
    state = HLTV_SCRAPER.get_state()
    return jsonify({
        'watching': HLTV_SCRAPER.is_watching,
        'state': state,
    })


@app.route('/api/stop_watch', methods=['POST'])
def api_stop_watch():
    if HLTV_SCRAPER:
        HLTV_SCRAPER.stop()
    return jsonify({'ok': True})


@app.route('/api/team_list')
def api_team_list():
    model, encoders, scale = load_model()
    teams = sorted(encoders['team'].categories_[0].tolist())
    return jsonify({'teams': teams})


def _prob_to_american(prob):
    if prob <= 0 or prob >= 1:
        return '+0'
    if prob >= 0.5:
        return f"{int(round(-prob / (1 - prob) * 100))}"
    else:
        return f"+{int(round((1 - prob) / prob * 100))}"


@app.route('/api/predict', methods=['POST'])
def api_predict():
    data = request.json
    team_a = data.get('team_a', '')
    team_b = data.get('team_b', '')
    fmt = data.get('format', 'bo3')
    if not team_a or not team_b:
        return jsonify({'error': 'Need team_a and team_b'}), 400
    model, encoders, scale = load_model()
    try:
        raw_prob_a, _, _ = get_win_prob(model, encoders, team_a, team_b, scale)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    from train_model import load_matches, build_training_data
    df = build_training_data(load_matches())
    counts = df['team'].value_counts()

    if fmt == 'bo1':
        map_p = series_to_map_prob(raw_prob_a)
        prob_a = map_p
        prob_b = 1 - prob_a
        over_under = None
    elif fmt == 'bo5':
        map_p = series_to_map_prob(raw_prob_a)
        q = 1 - map_p
        prob_a = map_p**3 * (1 + 3*q + 6*q*q)
        prob_b = 1 - prob_a
        p_3_2 = 6 * map_p**3 * q**2
        p_2_3 = 6 * q**3 * map_p**2
        over_prob = p_3_2 + p_2_3
        under_prob = 1 - over_prob
        over_under = {
            'line': '4.5',
            'over_prob': round(over_prob, 4),
            'under_prob': round(under_prob, 4),
            'over_fair': int(round(over_prob * 100)),
            'under_fair': int(round(under_prob * 100)),
        }
    else:
        prob_a = raw_prob_a
        prob_b = 1 - prob_a
        map_p = series_to_map_prob(prob_a)
        p_2_1 = 2 * map_p * map_p * (1 - map_p)
        p_1_2 = 2 * (1 - map_p) * (1 - map_p) * map_p
        over_prob = p_2_1 + p_1_2
        under_prob = 1 - over_prob
        over_under = {
            'line': '2.5',
            'over_prob': round(over_prob, 4),
            'under_prob': round(under_prob, 4),
            'over_fair': int(round(over_prob * 100)),
            'under_fair': int(round(under_prob * 100)),
        }

    result = {
        'team_a': team_a, 'team_b': team_b,
        'prob_a': round(prob_a, 4),
        'prob_b': round(prob_b, 4),
        'fair_a': int(round(prob_a * 100)),
        'fair_b': 100 - int(round(prob_a * 100)),
        'ml_a': _prob_to_american(prob_a),
        'ml_b': _prob_to_american(prob_b),
        'games_a': int(counts.get(team_a, 0)),
        'games_b': int(counts.get(team_b, 0)),
    }
    if over_under:
        result['over_under'] = over_under
    return jsonify(result)


@app.route('/api/bracket_simulate', methods=['POST'])
def api_bracket_simulate():
    import numpy as np
    data = request.json
    rounds_data = data['rounds']
    swiss_stages_data = data.get('swiss_stages', [])
    n_sims = min(data.get('sims', 100000), 500000)
    has_swiss = len(swiss_stages_data) > 0

    teams = []
    seen = set()

    for st in swiss_stages_data:
        for slot in st['slots']:
            if slot['type'] == 'team' and slot['team'] not in seen:
                teams.append(slot['team'])
                seen.add(slot['team'])

    entry_round = {}
    for r_idx, rnd in enumerate(rounds_data):
        for game in rnd['games']:
            for slot in [game['slot1'], game['slot2']]:
                if slot['type'] == 'team' and slot['team'] not in seen:
                    teams.append(slot['team'])
                    seen.add(slot['team'])
                    entry_round[slot['team']] = r_idx

    n = len(teams)
    if n < 2:
        return jsonify({'error': 'Need at least 2 teams'})

    team_idx_map = {t: i for i, t in enumerate(teams)}
    n_rounds = len(rounds_data)

    from kalshi_edge import series_prob_to_map_prob

    model, encoders, scale = load_model()

    map_wp = np.full((n, n), 0.5)
    for i in range(n):
        for j in range(i + 1, n):
            try:
                bo3_prob, _, _ = get_win_prob(model, encoders, teams[i], teams[j], scale)
                p = series_prob_to_map_prob(bo3_prob)
            except Exception:
                p = 0.5
            map_wp[i][j] = p
            map_wp[j][i] = 1.0 - p

    def map_prob_to_series(p, bo):
        if bo == 1:
            return p
        elif bo == 3:
            return p * p * (3 - 2 * p)
        elif bo == 5:
            return p**3 * (6*p*p - 15*p + 10)
        return p * p * (3 - 2 * p)

    bo_formats = {1, 3}
    for rnd in rounds_data:
        for game in rnd['games']:
            bo_formats.add(game.get('bo', 3))

    wp_by_bo = {}
    for bo in bo_formats:
        wp = np.full((n, n), 0.5)
        for i in range(n):
            for j in range(i + 1, n):
                wp[i][j] = map_prob_to_series(map_wp[i][j], bo)
                wp[j][i] = 1.0 - wp[i][j]
        wp_by_bo[bo] = wp

    advance = np.zeros((n, n_rounds + 1), dtype=int)
    for t, er in entry_round.items():
        idx = team_idx_map[t]
        for col in range(er + 1):
            advance[idx, col] = n_sims

    n_swiss_stages = len(swiss_stages_data)
    stage_advance_count = np.zeros((max(n_swiss_stages, 1), n), dtype=int)
    stage_record_count = {}
    for si, st in enumerate(swiss_stages_data):
        w2a = st['wins_to_advance']
        l2e = st['losses_to_eliminate']
        stage_record_count[si] = {}
        for w in range(w2a + 1):
            for l in range(l2e + 1):
                if w == w2a or l == l2e:
                    stage_record_count[si][(w, l)] = np.zeros(n, dtype=int)

    total_games = sum(len(rnd['games']) for rnd in rounds_data)
    all_rng = np.random.random((n_sims, total_games))

    for sim in range(n_sims):
        swiss_advancers = {}

        if has_swiss:
            for si, st in enumerate(swiss_stages_data):
                w2a = st['wins_to_advance']
                l2e = st['losses_to_eliminate']

                direct_teams = []
                adv_slots = []
                for slot in st['slots']:
                    if slot['type'] == 'team':
                        direct_teams.append(team_idx_map[slot['team']])
                    else:
                        adv_slots.append(slot['stage'])

                adv_pool = {}
                for fs in adv_slots:
                    if fs not in adv_pool:
                        adv_pool[fs] = list(swiss_advancers.get(fs, []))
                        np.random.shuffle(adv_pool[fs])

                adv_cursor = {fs: 0 for fs in adv_pool}
                participants = list(direct_teams)
                for fs in adv_slots:
                    cursor = adv_cursor[fs]
                    pool = adv_pool[fs]
                    if cursor < len(pool):
                        participants.append(pool[cursor])
                        adv_cursor[fs] = cursor + 1

                seen_p = set()
                unique_participants = []
                for t in participants:
                    if t not in seen_p:
                        seen_p.add(t)
                        unique_participants.append(t)
                participants = unique_participants

                num_p = len(participants)
                if num_p < 2:
                    swiss_advancers[si] = participants
                    for t in participants:
                        stage_advance_count[si][t] += 1
                    continue

                seeds = {participants[i]: i for i in range(num_p)}
                wins = np.zeros(num_p, dtype=int)
                losses = np.zeros(num_p, dtype=int)

                if st.get('mid_stage'):
                    slot_records = {}
                    for slot in st['slots']:
                        if slot['type'] == 'team':
                            ti = team_idx_map.get(slot['team'])
                            if ti is not None:
                                slot_records[ti] = (slot.get('wins', 0), slot.get('losses', 0))
                    for pi in range(num_p):
                        t = participants[pi]
                        if t in slot_records:
                            wins[pi] = slot_records[t][0]
                            losses[pi] = slot_records[t][1]

                forced_pairs = []
                if st.get('mid_stage') and st.get('next_matchups'):
                    for mu in st['next_matchups']:
                        t1i = team_idx_map.get(mu.get('team1'))
                        t2i = team_idx_map.get(mu.get('team2'))
                        if t1i is not None and t2i is not None:
                            forced_pairs.append((t1i, t2i))

                active = []
                advanced = []
                eliminated = []
                for pi in range(num_p):
                    if wins[pi] >= w2a:
                        advanced.append(pi)
                    elif losses[pi] >= l2e:
                        eliminated.append(pi)
                    else:
                        active.append(pi)

                first_round = True
                max_rounds = w2a + l2e - 1
                for _ in range(max_rounds):
                    if len(active) < 2:
                        break
                    pools = {}
                    for pi in active:
                        key = (wins[pi], losses[pi])
                        if key not in pools:
                            pools[key] = []
                        pools[key].append(pi)

                    use_forced = first_round and len(forced_pairs) > 0
                    first_round = False

                    for key in pools:
                        pool = pools[key]
                        pool.sort(key=lambda pi: seeds[participants[pi]])
                        is_adv_match = wins[pool[0]] == w2a - 1
                        is_elim_match = losses[pool[0]] == l2e - 1
                        match_bo = st.get('match_bo', 1)
                        decider_bo = st.get('decider_bo', 3)
                        bo = decider_bo if (is_adv_match or is_elim_match) else match_bo
                        if bo not in wp_by_bo:
                            bo_formats.add(bo)
                            wp_new = np.full((n, n), 0.5)
                            for ii in range(n):
                                for jj in range(ii + 1, n):
                                    wp_new[ii][jj] = map_prob_to_series(map_wp[ii][jj], bo)
                                    wp_new[jj][ii] = 1.0 - wp_new[ii][jj]
                            wp_by_bo[bo] = wp_new
                        wp = wp_by_bo[bo]

                        if use_forced:
                            pool_set = set(participants[pi] for pi in pool)
                            matched = set()
                            for ft1, ft2 in forced_pairs:
                                if ft1 in pool_set and ft2 in pool_set and ft1 not in matched and ft2 not in matched:
                                    p1 = next(pi for pi in pool if participants[pi] == ft1)
                                    p2 = next(pi for pi in pool if participants[pi] == ft2)
                                    matched.add(ft1)
                                    matched.add(ft2)
                                    if np.random.random() < wp[ft1, ft2]:
                                        winner, loser = p1, p2
                                    else:
                                        winner, loser = p2, p1
                                    wins[winner] += 1
                                    losses[loser] += 1
                                    if wins[winner] >= w2a:
                                        advanced.append(winner)
                                    if losses[loser] >= l2e:
                                        eliminated.append(loser)
                            remaining = [pi for pi in pool if participants[pi] not in matched]
                            half = len(remaining) // 2
                            for mi in range(half):
                                p1 = remaining[mi]
                                p2 = remaining[len(remaining) - 1 - mi]
                                t1 = participants[p1]
                                t2 = participants[p2]
                                if np.random.random() < wp[t1, t2]:
                                    winner, loser = p1, p2
                                else:
                                    winner, loser = p2, p1
                                wins[winner] += 1
                                losses[loser] += 1
                                if wins[winner] >= w2a:
                                    advanced.append(winner)
                                if losses[loser] >= l2e:
                                    eliminated.append(loser)
                        else:
                            half = len(pool) // 2
                            for mi in range(half):
                                p1 = pool[mi]
                                p2 = pool[len(pool) - 1 - mi]
                                t1 = participants[p1]
                                t2 = participants[p2]
                                if np.random.random() < wp[t1, t2]:
                                    winner, loser = p1, p2
                                else:
                                    winner, loser = p2, p1
                                wins[winner] += 1
                                losses[loser] += 1
                                if wins[winner] >= w2a:
                                    advanced.append(winner)
                                if losses[loser] >= l2e:
                                    eliminated.append(loser)
                    active = [pi for pi in range(num_p) if pi not in advanced and pi not in eliminated
                              and wins[pi] < w2a and losses[pi] < l2e]

                for pi in active:
                    if wins[pi] >= w2a:
                        advanced.append(pi)
                    else:
                        eliminated.append(pi)

                advanced.sort(key=lambda pi: (-wins[pi], losses[pi], seeds[participants[pi]]))
                adv_team_ids = [participants[pi] for pi in advanced]
                swiss_advancers[si] = adv_team_ids

                for pi in advanced:
                    t = participants[pi]
                    stage_advance_count[si][t] += 1
                    rec = (int(wins[pi]), int(losses[pi]))
                    if rec in stage_record_count[si]:
                        stage_record_count[si][rec][t] += 1
                for pi in eliminated:
                    t = participants[pi]
                    rec = (int(wins[pi]), int(losses[pi]))
                    if rec in stage_record_count[si]:
                        stage_record_count[si][rec][t] += 1

        def resolve_slot(slot, round_winners, round_losers):
            if slot['type'] == 'team':
                return team_idx_map[slot['team']]
            elif slot['type'] == 'winner':
                return round_winners[slot['round']][slot['game']]
            elif slot['type'] == 'loser':
                return round_losers[slot['round']][slot['game']]
            elif slot['type'] == 'swiss_advance':
                advs = swiss_advancers.get(slot['stage'], [])
                seed = slot['seed']
                return advs[seed] if seed < len(advs) else -1
            return -1

        rng = all_rng[sim]
        gi = 0
        round_winners = []
        round_losers = []
        for r_idx, rnd in enumerate(rounds_data):
            winners = []
            losers = []
            for game in rnd['games']:
                t1 = resolve_slot(game['slot1'], round_winners, round_losers)
                t2 = resolve_slot(game['slot2'], round_winners, round_losers)
                if t1 < 0 or t2 < 0:
                    w = t1 if t1 >= 0 else t2
                    winners.append(w)
                    losers.append(-1)
                    if w >= 0:
                        advance[w, r_idx + 1] += 1
                    gi += 1
                    continue
                bo = game.get('bo', 3)
                wp = wp_by_bo[bo]
                winner = t1 if rng[gi] < wp[t1, t2] else t2
                loser = t2 if winner == t1 else t1
                winners.append(winner)
                losers.append(loser)
                advance[winner, r_idx + 1] += 1
                gi += 1
            round_winners.append(winners)
            round_losers.append(losers)

    pct = advance / n_sims
    labels = [rnd.get('label', f'Rd {i+1}') for i, rnd in enumerate(rounds_data)]
    order = sorted(range(n), key=lambda i: tuple(-pct[i, r] for r in range(n_rounds, 0, -1)))

    results = []
    for idx in order:
        if all(pct[idx, r] == 0 for r in range(n_rounds + 1)):
            continue
        row = {'team': teams[idx], 'rounds': {}}
        for r in range(1, n_rounds + 1):
            if teams[idx] in entry_round and r <= entry_round[teams[idx]]:
                row['rounds'][labels[r - 1]] = 'bye'
            else:
                row['rounds'][labels[r - 1]] = round(float(pct[idx, r]), 4)
        results.append(row)

    results = [r for r in results if any(v != 0 and v != 'bye' for v in r['rounds'].values())]

    response = {'results': results, 'labels': labels, 'sims': n_sims}

    if has_swiss:
        swiss_results = []
        for si, st in enumerate(swiss_stages_data):
            w2a = st['wins_to_advance']
            l2e = st['losses_to_eliminate']
            involved = set()
            for slot in st['slots']:
                if slot['type'] == 'team':
                    involved.add(team_idx_map[slot['team']])
            for fs_advs in stage_record_count[si].values():
                for ti in range(n):
                    if fs_advs[ti] > 0:
                        involved.add(ti)
            team_results = []
            for ti in sorted(involved, key=lambda t: -stage_advance_count[si][t]):
                adv_pct = stage_advance_count[si][ti] / n_sims if n_sims > 0 else 0
                records = {}
                for (w, l), counts in stage_record_count[si].items():
                    if counts[ti] > 0:
                        records[f'{w}-{l}'] = round(counts[ti] / n_sims, 4)
                team_results.append({
                    'team': teams[ti],
                    'advance_pct': round(adv_pct, 4),
                    'records': records,
                })
            swiss_results.append({
                'label': st.get('label', f'Stage {si+1}'),
                'wins_to_advance': w2a,
                'losses_to_eliminate': l2e,
                'results': team_results,
            })
        response['swiss_stages'] = swiss_results

    return jsonify(response)


# ── Futures / Props Trading API ──────────────────────────────────

@app.route('/api/futures/fetch_event', methods=['POST'])
def api_futures_fetch_event():
    data = request.json
    url = data.get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL provided'})

    if url.lower() == 'kalshi':
        return _fetch_kalshi_cs2_futures()
    elif 'polymarket.com' in url or (not url.startswith('KALSHI:') and 'kalshi.com' not in url):
        return _fetch_poly_futures(url)
    else:
        return _fetch_kalshi_futures(url)


def _fetch_poly_futures(url):
    m = re.search(r'polymarket\.com/event/([a-z0-9_-]+)', url)
    slug = m.group(1) if m else url.strip('/').split('/')[-1]

    try:
        resp = http_requests.get(f"{GAMMA_BASE}/events",
                                  params={'slug': slug}, timeout=15)
        if resp.status_code != 200:
            return jsonify({'error': f'Gamma API error: {resp.status_code}'})
        events = resp.json()
        if not events:
            return jsonify({'error': f'Event not found: {slug}'})
        event = events[0] if isinstance(events, list) else events
    except Exception as e:
        return jsonify({'error': f'Fetch failed: {e}'})

    markets_out = []
    token_ids_to_fetch = []

    for mkt in event.get('markets', []):
        outcomes = json.loads(mkt.get('outcomes', '[]'))
        prices = json.loads(mkt.get('outcomePrices', '[]'))
        tokens = json.loads(mkt.get('clobTokenIds', '[]'))
        if len(outcomes) < 2 or len(tokens) < 2 or len(prices) < 2:
            continue
        if prices in [['0', '1'], ['1', '0']]:
            continue

        team = mkt.get('groupItemTitle') or ''
        if not team:
            q = mkt.get('question', '')
            qm = re.match(r'^Will\s+(.+?)\s+(?:win|qualify|advance|make|finish)\b',
                           q, re.IGNORECASE)
            team = qm.group(1) if qm else outcomes[0]

        markets_out.append({
            'team': team,
            'market_price': float(prices[0]) if prices[0] else 0,
            'token_yes': tokens[0],
            'token_no': tokens[1],
            'token_id': tokens[0],
            'tick_size': str(mkt.get('orderPriceMinTickSize', '0.01')),
            'neg_risk': mkt.get('negRisk', False),
            'market_id': mkt.get('id', ''),
            'platform': 'poly',
            'best_ask_yes': None,
            'best_ask_no': None,
            'best_ask': None,
        })
        token_ids_to_fetch.append(tokens[0])
        token_ids_to_fetch.append(tokens[1])

    def _get_ask(token_id):
        ob = get_poly_orderbook(token_id)
        return token_id, poly_best_ask(ob)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(_get_ask, tid): tid for tid in token_ids_to_fetch}
        for f in as_completed(futs):
            try:
                tid, ask = f.result()
                for mk in markets_out:
                    if mk['token_yes'] == tid:
                        mk['best_ask_yes'] = ask
                        mk['best_ask'] = ask
                    elif mk['token_no'] == tid:
                        mk['best_ask_no'] = ask
            except Exception:
                pass

    markets_out.sort(key=lambda x: -x['market_price'])
    return jsonify({'markets': markets_out, 'event_title': event.get('title', slug)})


def _fetch_kalshi_cs2_futures():
    """Return list of CS2 futures events (no markets yet — user picks one first)."""
    MATCH_SERIES = ('KXCS2GAME', 'KXCS2MAP', 'KXCS2TOTALMAPS')
    CS2_SERIES = ['KXCS2', 'KXCS2QUALIFIERS']
    try:
        hdrs = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'GET',
                                  '/trade-api/v2/events') if PRIVATE_KEY else {}
        all_events = []
        for series in CS2_SERIES:
            cursor = None
            while True:
                params = {'series_ticker': series, 'status': 'open', 'limit': 200}
                if cursor:
                    params['cursor'] = cursor
                resp = http_requests.get(f"{KALSHI_BASE}/events", headers=hdrs,
                                          params=params, timeout=10)
                if resp.status_code != 200:
                    break
                data = resp.json()
                batch = data.get('events', [])
                for ev in batch:
                    et = ev.get('event_ticker', '')
                    if not any(et.startswith(p) for p in MATCH_SERIES):
                        all_events.append({
                            'event_ticker': et,
                            'title': ev.get('title') or ev.get('sub_title') or et,
                            'series_ticker': ev.get('series_ticker', series),
                        })
                cursor = data.get('cursor', '')
                if not cursor or not batch:
                    break

        if not all_events:
            return jsonify({'error': 'No CS2 futures events found on Kalshi'})
    except Exception as e:
        return jsonify({'error': f'Fetch failed: {e}'})

    return jsonify({'mode': 'event_list', 'events': all_events})


def _fetch_kalshi_futures(url):
    ticker = url.replace('KALSHI:', '').strip()
    if 'kalshi.com' in ticker:
        parts = ticker.split('/')
        ticker = parts[-1] if parts else ticker

    try:
        hdrs = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'GET',
                                  '/trade-api/v2/markets') if PRIVATE_KEY else {}
        raw_markets = []

        # 1) Try as event_ticker
        params = {'event_ticker': ticker, 'status': 'open', 'limit': 200}
        resp = http_requests.get(f"{KALSHI_BASE}/markets", headers=hdrs,
                                  params=params, timeout=10)
        if resp.status_code == 200:
            raw_markets = resp.json().get('markets', [])

        # 2) If that failed and ticker has 2+ dashes, it may be a market ticker —
        #    derive event ticker by stripping the last segment
        if not raw_markets and ticker.count('-') >= 2:
            event_ticker = '-'.join(ticker.split('-')[:-1])
            params = {'event_ticker': event_ticker, 'status': 'open', 'limit': 200}
            resp = http_requests.get(f"{KALSHI_BASE}/markets", headers=hdrs,
                                      params=params, timeout=10)
            if resp.status_code == 200:
                raw_markets = resp.json().get('markets', [])

        # 3) Try as series_ticker
        if not raw_markets:
            params = {'series_ticker': ticker, 'status': 'open', 'limit': 200}
            resp = http_requests.get(f"{KALSHI_BASE}/markets", headers=hdrs,
                                      params=params, timeout=10)
            if resp.status_code == 200:
                raw_markets = resp.json().get('markets', [])

        # 4) Try as a single market ticker directly
        if not raw_markets:
            resp = http_requests.get(f"{KALSHI_BASE}/markets/{ticker}",
                                      headers=hdrs, timeout=10)
            if resp.status_code == 200:
                mkt = resp.json().get('market')
                if mkt:
                    raw_markets = [mkt]

        if not raw_markets:
            return jsonify({'error': f'No markets found for {ticker}'})
    except Exception as e:
        return jsonify({'error': f'Fetch failed: {e}'})

    markets_out = []

    def _process_market(mkt):
        t = mkt.get('ticker', '')
        title = mkt.get('title', '')
        yes_price = float(mkt.get('yes_bid', 0)) / 100 if mkt.get('yes_bid') else 0
        team_match = re.match(r'^Will\s+(.+?)\s+(?:win|qualify|advance|make|finish)\b',
                              title, re.IGNORECASE)
        team = team_match.group(1) if team_match else title[:50]

        ob = fetch_orderbook_best_ask(t)
        best_ask_yes = None
        best_ask_no = None
        if ob:
            if ob.get('yes_best_ask') is not None:
                best_ask_yes = ob['yes_best_ask'] / 100
            if ob.get('no_best_ask') is not None:
                best_ask_no = ob['no_best_ask'] / 100

        return {
            'team': team,
            'market_price': yes_price,
            'ticker': t,
            'platform': 'kalshi',
            'best_ask': best_ask_yes,
            'best_ask_yes': best_ask_yes,
            'best_ask_no': best_ask_no,
        }

    with ThreadPoolExecutor(max_workers=8) as pool:
        for f in as_completed([pool.submit(_process_market, m) for m in raw_markets]):
            try:
                markets_out.append(f.result())
            except Exception:
                pass

    markets_out.sort(key=lambda x: -x['market_price'])
    return jsonify({'markets': markets_out, 'event_title': ticker})


@app.route('/api/futures/post_orders', methods=['POST'])
def api_futures_post_orders():
    data = request.json
    orders = data.get('orders', [])
    spread_cents = data.get('spread_cents', 5)
    size = data.get('size', 50)
    results = []

    per_layer = max(1, size // LAYER_COUNT)

    def _post_one(order):
        fair = order['fair_prob']
        platform = order['platform']
        team = order.get('team', '?')

        if platform == 'poly':
            token_yes = order.get('token_yes') or order.get('token_id')
            token_no = order.get('token_no')
            tick_size = order.get('tick_size', '0.01')
            neg_risk = order.get('neg_risk', False)
            tick = float(tick_size)

            posted = []

            # Yes side — layered
            base_yes = round(fair - spread_cents / 100, 2)
            ob_yes = get_poly_orderbook(token_yes)
            ask_yes = poly_best_ask(ob_yes)
            if base_yes >= ask_yes > 0:
                base_yes = round(ask_yes - tick, 2)

            if DRY_RUN or not POLY_CLIENT:
                for layer in range(LAYER_COUNT):
                    bid = round(base_yes - layer * tick, 2)
                    if bid >= tick:
                        posted.append(f'YES@{bid:.2f}x{per_layer}(dry)')
            else:
                POLY_CLIENT.cancel_token_orders(token_yes)
                for layer in range(LAYER_COUNT):
                    bid = round(base_yes - layer * tick, 2)
                    if bid >= tick:
                        resp = POLY_CLIENT.place_order(token_yes, "BUY", bid, per_layer,
                                                        tick_size=tick_size, neg_risk=neg_risk)
                        posted.append(f'YES@{bid:.2f}x{per_layer}' if resp else f'YES@{bid:.2f}:FAIL')
            with FUTURES_TOKEN_LOCK:
                FUTURES_TOKEN_IDS.append(token_yes)

            # No side — layered
            if token_no:
                fair_no = 1.0 - fair
                base_no = round(fair_no - spread_cents / 100, 2)
                ob_no = get_poly_orderbook(token_no)
                ask_no = poly_best_ask(ob_no)
                if base_no >= ask_no > 0:
                    base_no = round(ask_no - tick, 2)

                if DRY_RUN or not POLY_CLIENT:
                    for layer in range(LAYER_COUNT):
                        bid = round(base_no - layer * tick, 2)
                        if bid >= tick:
                            posted.append(f'NO@{bid:.2f}x{per_layer}(dry)')
                else:
                    POLY_CLIENT.cancel_token_orders(token_no)
                    for layer in range(LAYER_COUNT):
                        bid = round(base_no - layer * tick, 2)
                        if bid >= tick:
                            resp = POLY_CLIENT.place_order(token_no, "BUY", bid, per_layer,
                                                            tick_size=tick_size, neg_risk=neg_risk)
                            posted.append(f'NO@{bid:.2f}x{per_layer}' if resp else f'NO@{bid:.2f}:FAIL')
                with FUTURES_TOKEN_LOCK:
                    FUTURES_TOKEN_IDS.append(token_no)

            if not posted:
                return {'team': team, 'status': 'skipped', 'reason': 'bids too low'}
            status = 'dry-run' if (DRY_RUN or not POLY_CLIENT) else 'placed'
            return {'team': team, 'sides': ', '.join(posted), 'status': status}

        elif platform == 'kalshi':
            from kalshi_edge import place_limit_order
            ticker = order['ticker']
            ob = fetch_orderbook_best_ask(ticker)
            posted = []

            # Yes side — layered
            base_yes = max(1, round(fair * 100) - spread_cents)
            if ob and ob.get('yes_best_ask') is not None:
                if base_yes >= ob['yes_best_ask']:
                    base_yes = ob['yes_best_ask'] - 1

            for layer in range(LAYER_COUNT):
                bid = base_yes - layer
                if bid >= 1:
                    if DRY_RUN:
                        posted.append(f'YES@{bid}cx{per_layer}(dry)')
                    else:
                        resp = place_limit_order(API_KEY_ID, PRIVATE_KEY, ticker, 'yes',
                                                  per_layer, bid, DRY_RUN)
                        posted.append(f'YES@{bid}cx{per_layer}' if resp else f'YES@{bid}c:FAIL')

            # No side — layered
            fair_no = 1.0 - fair
            base_no = max(1, round(fair_no * 100) - spread_cents)
            if ob and ob.get('no_best_ask') is not None:
                if base_no >= ob['no_best_ask']:
                    base_no = ob['no_best_ask'] - 1

            for layer in range(LAYER_COUNT):
                bid = base_no - layer
                if bid >= 1:
                    if DRY_RUN:
                        posted.append(f'NO@{bid}cx{per_layer}(dry)')
                    else:
                        resp = place_limit_order(API_KEY_ID, PRIVATE_KEY, ticker, 'no',
                                                  per_layer, bid, DRY_RUN)
                        posted.append(f'NO@{bid}cx{per_layer}' if resp else f'NO@{bid}c:FAIL')

            if not posted:
                return {'team': team, 'status': 'skipped'}
            status = 'dry-run' if DRY_RUN else 'placed'
            return {'team': team, 'sides': ', '.join(posted), 'status': status}

    with ThreadPoolExecutor(max_workers=8) as pool:
        for f in as_completed([pool.submit(_post_one, o) for o in orders]):
            try:
                results.append(f.result())
            except Exception as e:
                results.append({'status': 'error', 'error': str(e)})

    placed = sum(1 for r in results if r.get('status') in ('placed', 'dry-run'))
    return jsonify({
        'results': results,
        'message': f'Posted {placed}/{len(orders)} orders',
    })


@app.route('/api/futures/cancel_all', methods=['POST'])
def api_futures_cancel_all():
    data = request.json
    token_ids = data.get('token_ids', [])
    tickers = data.get('tickers', [])
    messages = []

    if token_ids and POLY_CLIENT:
        cancelled = 0
        for tid in token_ids:
            try:
                POLY_CLIENT.cancel_token_orders(tid)
                cancelled += 1
            except Exception:
                pass
        messages.append(f'Poly: cancelled {cancelled}/{len(token_ids)} markets')
    elif token_ids:
        messages.append('Poly: no client (dry run)')

    if tickers and PRIVATE_KEY:
        from kalshi_edge import cancel_orders_for_ticker
        cancelled = 0
        for t in tickers:
            cancelled += cancel_orders_for_ticker(API_KEY_ID, PRIVATE_KEY, t)
        messages.append(f'Kalshi: cancelled {cancelled} orders')

    with FUTURES_TOKEN_LOCK:
        FUTURES_TOKEN_IDS.clear()

    return jsonify({'message': ' | '.join(messages) or 'Nothing to cancel'})


@app.route('/api/swiss_simulate', methods=['POST'])
def api_swiss_simulate():
    import numpy as np
    data = request.json
    stages_data = data['stages']
    n_sims = min(data.get('sims', 100000), 500000)

    all_teams = set()
    for st in stages_data:
        for slot in st['slots']:
            if slot['type'] == 'team':
                all_teams.add(slot['team'])
    all_teams = sorted(all_teams)
    n = len(all_teams)
    if n < 2:
        return jsonify({'error': 'Need at least 2 teams'})

    team_idx = {t: i for i, t in enumerate(all_teams)}

    from kalshi_edge import series_prob_to_map_prob
    model, encoders, scale = load_model()

    map_wp = np.full((n, n), 0.5)
    for i in range(n):
        for j in range(i + 1, n):
            try:
                bo3_prob, _, _ = get_win_prob(model, encoders, all_teams[i], all_teams[j], scale)
                p = series_prob_to_map_prob(bo3_prob)
            except Exception:
                p = 0.5
            map_wp[i][j] = p
            map_wp[j][i] = 1.0 - p

    def map_prob_to_series(p, bo):
        if bo == 1:
            return p
        elif bo == 3:
            return p * p * (3 - 2 * p)
        elif bo == 5:
            return p**3 * (6*p*p - 15*p + 10)
        return p * p * (3 - 2 * p)

    wp_by_bo = {}
    wp_by_bo[1] = np.copy(map_wp)
    all_bos = {1, 3}
    for st in stages_data:
        all_bos.add(st.get('match_bo', 1))
        all_bos.add(st.get('decider_bo', 3))
    for bo in all_bos:
        if bo not in wp_by_bo:
            wp_new = np.full((n, n), 0.5)
            for i in range(n):
                for j in range(i + 1, n):
                    wp_new[i][j] = map_prob_to_series(map_wp[i][j], bo)
                    wp_new[j][i] = 1.0 - wp_new[i][j]
            wp_by_bo[bo] = wp_new

    n_stages = len(stages_data)
    stage_advance_count = np.zeros((n_stages, n), dtype=int)
    stage_record_count = {}
    for si, st in enumerate(stages_data):
        w2a = st['wins_to_advance']
        l2e = st['losses_to_eliminate']
        stage_record_count[si] = {}
        for w in range(w2a + 1):
            for l in range(l2e + 1):
                if w == w2a or l == l2e:
                    stage_record_count[si][(w, l)] = np.zeros(n, dtype=int)

    for sim in range(n_sims):
        stage_advancers = {}

        for si, st in enumerate(stages_data):
            w2a = st['wins_to_advance']
            l2e = st['losses_to_eliminate']

            direct_teams = []
            adv_slots = []
            for slot in st['slots']:
                if slot['type'] == 'team':
                    direct_teams.append(team_idx[slot['team']])
                else:
                    from_stage = slot['stage']
                    adv_slots.append(from_stage)

            adv_pool = {}
            for fs in adv_slots:
                if fs not in adv_pool:
                    adv_pool[fs] = list(stage_advancers.get(fs, []))
                    np.random.shuffle(adv_pool[fs])

            adv_cursor = {fs: 0 for fs in adv_pool}
            participants = list(direct_teams)
            for fs in adv_slots:
                cursor = adv_cursor[fs]
                pool = adv_pool[fs]
                if cursor < len(pool):
                    participants.append(pool[cursor])
                    adv_cursor[fs] = cursor + 1

            seen = set()
            unique_participants = []
            for t in participants:
                if t not in seen:
                    seen.add(t)
                    unique_participants.append(t)
            participants = unique_participants

            num_p = len(participants)
            if num_p < 2:
                stage_advancers[si] = participants
                for t in participants:
                    stage_advance_count[si][t] += 1
                continue

            seeds = {participants[i]: i for i in range(num_p)}
            wins = np.zeros(num_p, dtype=int)
            losses = np.zeros(num_p, dtype=int)

            if st.get('mid_stage'):
                slot_records = {}
                for slot in st['slots']:
                    if slot['type'] == 'team':
                        ti = team_idx.get(slot['team'])
                        if ti is not None:
                            slot_records[ti] = (slot.get('wins', 0), slot.get('losses', 0))
                for pi in range(num_p):
                    t = participants[pi]
                    if t in slot_records:
                        wins[pi] = slot_records[t][0]
                        losses[pi] = slot_records[t][1]

            forced_pairs = []
            if st.get('mid_stage') and st.get('next_matchups'):
                for mu in st['next_matchups']:
                    t1i = team_idx.get(mu.get('team1'))
                    t2i = team_idx.get(mu.get('team2'))
                    if t1i is not None and t2i is not None:
                        forced_pairs.append((t1i, t2i))

            active = []
            advanced = []
            eliminated = []
            for pi in range(num_p):
                if wins[pi] >= w2a:
                    advanced.append(pi)
                elif losses[pi] >= l2e:
                    eliminated.append(pi)
                else:
                    active.append(pi)

            first_round = True
            max_rounds = w2a + l2e - 1
            for _ in range(max_rounds):
                if len(active) < 2:
                    break

                pools = {}
                for pi in active:
                    key = (wins[pi], losses[pi])
                    if key not in pools:
                        pools[key] = []
                    pools[key].append(pi)

                use_forced = first_round and len(forced_pairs) > 0
                first_round = False

                for key in pools:
                    pool = pools[key]
                    pool.sort(key=lambda pi: seeds[participants[pi]])

                    is_adv_match = wins[pool[0]] == w2a - 1
                    is_elim_match = losses[pool[0]] == l2e - 1
                    match_bo = st.get('match_bo', 1)
                    decider_bo = st.get('decider_bo', 3)
                    bo = decider_bo if (is_adv_match or is_elim_match) else match_bo
                    wp = wp_by_bo.get(bo, wp_by_bo[1])

                    if use_forced:
                        pool_set = set(participants[pi] for pi in pool)
                        matched = set()
                        for ft1, ft2 in forced_pairs:
                            if ft1 in pool_set and ft2 in pool_set and ft1 not in matched and ft2 not in matched:
                                p1 = next(pi for pi in pool if participants[pi] == ft1)
                                p2 = next(pi for pi in pool if participants[pi] == ft2)
                                matched.add(ft1)
                                matched.add(ft2)
                                if np.random.random() < wp[ft1, ft2]:
                                    winner, loser = p1, p2
                                else:
                                    winner, loser = p2, p1
                                wins[winner] += 1
                                losses[loser] += 1
                                if wins[winner] >= w2a:
                                    advanced.append(winner)
                                if losses[loser] >= l2e:
                                    eliminated.append(loser)
                        remaining = [pi for pi in pool if participants[pi] not in matched]
                        half = len(remaining) // 2
                        for mi in range(half):
                            p1 = remaining[mi]
                            p2 = remaining[len(remaining) - 1 - mi]
                            t1 = participants[p1]
                            t2 = participants[p2]
                            if np.random.random() < wp[t1, t2]:
                                winner, loser = p1, p2
                            else:
                                winner, loser = p2, p1
                            wins[winner] += 1
                            losses[loser] += 1
                            if wins[winner] >= w2a:
                                advanced.append(winner)
                            if losses[loser] >= l2e:
                                eliminated.append(loser)
                    else:
                        half = len(pool) // 2
                        for mi in range(half):
                            p1 = pool[mi]
                            p2 = pool[len(pool) - 1 - mi]
                            t1 = participants[p1]
                            t2 = participants[p2]

                            if np.random.random() < wp[t1, t2]:
                                winner, loser = p1, p2
                            else:
                                winner, loser = p2, p1

                            wins[winner] += 1
                            losses[loser] += 1

                            if wins[winner] >= w2a:
                                advanced.append(winner)
                            if losses[loser] >= l2e:
                                eliminated.append(loser)

                    if len(pool) % 2 == 1:
                        leftover = pool[half] if not use_forced else (pool[-1] if len(pool) % 2 == 1 else None)
                        pass

                active = [pi for pi in range(num_p) if pi not in advanced and pi not in eliminated
                          and wins[pi] < w2a and losses[pi] < l2e]

            for pi in active:
                if wins[pi] >= w2a:
                    advanced.append(pi)
                else:
                    eliminated.append(pi)

            adv_team_ids = [participants[pi] for pi in advanced]
            stage_advancers[si] = adv_team_ids
            for pi in advanced:
                t = participants[pi]
                stage_advance_count[si][t] += 1
                rec = (int(wins[pi]), int(losses[pi]))
                if rec in stage_record_count[si]:
                    stage_record_count[si][rec][t] += 1
            for pi in eliminated:
                t = participants[pi]
                rec = (int(wins[pi]), int(losses[pi]))
                if rec in stage_record_count[si]:
                    stage_record_count[si][rec][t] += 1

    result_stages = []
    for si, st in enumerate(stages_data):
        w2a = st['wins_to_advance']
        l2e = st['losses_to_eliminate']

        involved = set()
        for slot in st['slots']:
            if slot['type'] == 'team':
                involved.add(team_idx[slot['team']])
        for fs_advs in stage_record_count[si].values():
            for ti in range(n):
                if fs_advs[ti] > 0:
                    involved.add(ti)

        team_results = []
        for ti in sorted(involved, key=lambda t: -stage_advance_count[si][t]):
            adv_pct = stage_advance_count[si][ti] / n_sims if n_sims > 0 else 0
            records = {}
            for (w, l), counts in stage_record_count[si].items():
                if counts[ti] > 0:
                    records[f'{w}-{l}'] = round(counts[ti] / n_sims, 4)
            team_results.append({
                'team': all_teams[ti],
                'advance_pct': round(adv_pct, 4),
                'records': records,
            })

        result_stages.append({
            'label': st.get('label', f'Stage {si+1}'),
            'wins_to_advance': w2a,
            'losses_to_eliminate': l2e,
            'results': team_results,
        })

    return jsonify({'stages': result_stages, 'sims': n_sims})


# ── Kalshi WebSocket API ─────────────────────────────────────────────

@app.route('/api/ws_status')
def api_ws_status():
    if not KALSHI_WS:
        return jsonify({'connected': False, 'tickers': [], 'fills': []})
    with KALSHI_WS_FILLS_LOCK:
        recent = list(KALSHI_WS_FILLS[-20:])
    return jsonify({
        'connected': KALSHI_WS.connected,
        'tickers': list(KALSHI_WS.subscribed_tickers),
        'fills': recent,
    })


@app.route('/api/ws_subscribe', methods=['POST'])
def api_ws_subscribe():
    if not KALSHI_WS:
        return jsonify({'error': 'WebSocket not available'}), 400
    data = request.get_json(force=True)
    tickers = data.get('tickers', [])
    if tickers:
        KALSHI_WS.subscribe(tickers)
    return jsonify({'ok': True, 'subscribed': list(KALSHI_WS.subscribed_tickers)})


# ── Esports Market Fetch API ──────────────────────────────────────────

ESPORT_KALSHI_SERIES = {
    'valorant': ['KXVALWIN'],
    'lol': ['KXLOLWIN'],
    'dota2': ['KXDOTAWIN'],
}

ESPORT_POLY_SLUGS = {
    'valorant': 'valorant',
    'lol': 'league-of-legends',
    'dota2': 'dota-2',
}


def _fetch_esport_kalshi_markets(esport):
    series_list = ESPORT_KALSHI_SERIES.get(esport, [])
    if not series_list:
        return []
    all_markets = []
    for series in series_list:
        cursor = None
        while True:
            params = {'series_ticker': series, 'status': 'open', 'limit': 200}
            if cursor:
                params['cursor'] = cursor
            try:
                hdrs = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'GET',
                                        '/trade-api/v2/markets') if PRIVATE_KEY else {}
                resp = http_requests.get(f"{KALSHI_BASE}/markets", headers=hdrs,
                                        params=params, timeout=10)
            except Exception as e:
                print(f"  [Esport] Kalshi API error ({series}): {e}")
                break
            if resp.status_code != 200:
                print(f"  [Esport] Kalshi API {resp.status_code} for {series}")
                break
            data = resp.json()
            batch = data.get('markets', [])
            for m in batch:
                if m.get('status', '').lower() in ('closed', 'settled', 'finalized'):
                    continue
                all_markets.append(m)
            cursor = data.get('cursor', '')
            if not cursor or not batch:
                break
    return all_markets


def _fetch_esport_poly_markets(esport):
    slug = ESPORT_POLY_SLUGS.get(esport)
    if not slug:
        return []
    events = []
    try:
        for offset in range(0, 2000, 100):
            resp = http_requests.get(f"{GAMMA_BASE}/events",
                                    params={'series_slug': slug,
                                            'closed': 'false', 'limit': 100,
                                            'offset': offset},
                                    timeout=15)
            if resp.status_code != 200:
                break
            batch = resp.json()
            events.extend(batch)
            if len(batch) < 100:
                break
    except Exception as e:
        print(f"  [Esport] Poly API error ({slug}): {e}")
    return events


@app.route('/api/esports/fetch_games', methods=['POST'])
def api_esports_fetch_games():
    data = request.get_json(force=True) or {}
    esport = data.get('esport', '')
    if esport not in ESPORT_KALSHI_SERIES:
        return jsonify({'error': f'Unknown esport: {esport}', 'games': []})

    result = {}

    kalshi_markets = _fetch_esport_kalshi_markets(esport)
    for m in kalshi_markets:
        title = m.get('title', '') or ''
        subtitle = m.get('subtitle', '') or ''

        away_k, home_k = None, None
        for text in [title, subtitle]:
            match = re.search(r'the\s+(.+?)\s+vs\.?\s+(.+?)\s+(?:\w+\s+)?match', text, re.IGNORECASE)
            if match:
                away_k, home_k = match.group(1).strip(), match.group(2).strip()
                break
            text_clean = re.sub(r'\s*(Map\s*\d\s+Winner|Winner|Match Winner)\s*\??\s*$', '', text,
                               flags=re.IGNORECASE).strip()
            match2 = re.match(r'^(.+?)\s+(?:at|vs\.?)\s+(.+?)$', text_clean, re.IGNORECASE)
            if match2:
                away_k, home_k = match2.group(1).strip(), match2.group(2).strip()
                break
        if not away_k or not home_k:
            continue

        ticker = m.get('ticker', '')
        key = (home_k, away_k)
        if key not in result:
            result[key] = {
                'home': home_k, 'away': away_k,
                'tickers': [], 'polySeriesTickers': [],
            }

        yes_team = home_k
        match_yes = re.match(r'^Will\s+(.+?)\s+win\b', title, re.IGNORECASE)
        if match_yes:
            yes_name = match_yes.group(1).strip()
            if yes_name.lower() == away_k.lower():
                yes_team = away_k
            else:
                yes_team = home_k
        home_is_yes = (yes_team == home_k)

        result[key]['tickers'].append({
            'ticker': ticker,
            'home_is_yes': home_is_yes,
            'yes_team': home_k if home_is_yes else away_k,
            'no_team': away_k if home_is_yes else home_k,
        })

    poly_events = _fetch_esport_poly_markets(esport)
    for event in poly_events:
        if event.get('closed'):
            continue
        for mkt in event.get('markets', []):
            if mkt.get('sportsMarketType') != 'moneyline':
                continue
            outcomes = json.loads(mkt.get('outcomePrices', '[]'))
            if outcomes in [['0', '1'], ['1', '0']]:
                continue
            outcome_names = mkt.get('outcomes', '')
            if isinstance(outcome_names, str):
                try:
                    outcome_names = json.loads(outcome_names)
                except Exception:
                    outcome_names = []
            if len(outcome_names) < 2:
                continue
            team_a, team_b = outcome_names[0], outcome_names[1]
            tokens = json.loads(mkt.get('clobTokenIds', '[]'))
            if len(tokens) < 2:
                continue

            q = mkt.get('question', '') or event.get('title', '') or ''
            match_vs = re.search(r'(.+?)\s+vs\.?\s+(.+)', q, re.IGNORECASE)
            if match_vs:
                away_p, home_p = match_vs.group(1).strip(), match_vs.group(2).strip()
            else:
                away_p, home_p = team_a, team_b

            away_p = re.sub(r'\s*\(.*?\)\s*$', '', away_p).strip()
            home_p = re.sub(r'\s*\(.*?\)\s*$', '', home_p).strip()
            away_p = re.sub(r'^(?:Will\s+)', '', away_p, flags=re.IGNORECASE).strip()

            key = (home_p, away_p)
            rkey = (away_p, home_p)
            if rkey in result and key not in result:
                key = rkey
            if key not in result:
                result[key] = {
                    'home': key[0], 'away': key[1],
                    'tickers': [], 'polySeriesTickers': [],
                }

            price_a = float(outcomes[0]) if len(outcomes) > 0 else 0
            price_b = float(outcomes[1]) if len(outcomes) > 1 else 0
            result[key]['polySeriesTickers'].append({
                'token_a': tokens[0], 'token_b': tokens[1],
                'team_a': team_a, 'team_b': team_b,
                'home_team': key[0], 'away_team': key[1],
                'price_a': price_a, 'price_b': price_b,
                'title': mkt.get('question', ''),
                'platform': 'poly',
                'tick_size': float(mkt.get('minimum_tick_size', '0.01')),
                'neg_risk': mkt.get('negRisk', False),
            })

    games = []
    for key, g in result.items():
        home_prob = 0.5
        if g['tickers']:
            pass
        games.append({
            'home': g['home'],
            'away': g['away'],
            'home_prob': home_prob,
            'tickers': g['tickers'],
            'polySeriesTickers': g['polySeriesTickers'],
        })

    games.sort(key=lambda g: g['away'].lower())
    print(f"  [Esport] {esport}: found {len(games)} games "
          f"({sum(len(g['tickers']) for g in games)} Kalshi, "
          f"{sum(len(g['polySeriesTickers']) for g in games)} Poly)")
    return jsonify({'games': games, 'esport': esport})


# ── Screen Tracker API ───────────────────────────────────────────────

@app.route('/api/st_screenshot', methods=['POST'])
def api_st_screenshot():
    data = request.get_json(force=True) or {}
    fast = data.get('fast', False)
    try:
        result = SCREEN_TRACKER.capture_full_screen(fast=fast)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/st_set_scoreboard', methods=['POST'])
def api_st_set_scoreboard():
    data = request.get_json(force=True)
    SCREEN_TRACKER.set_scoreboard(data)
    return jsonify({'ok': True})


@app.route('/api/st_scoreboard_capture', methods=['POST'])
def api_st_scoreboard_capture():
    result = SCREEN_TRACKER.capture_scoreboard()
    if result is None:
        return jsonify({'error': 'no scoreboard set'}), 400
    return jsonify(result)


@app.route('/api/st_set_sub_region', methods=['POST'])
def api_st_set_sub_region():
    data = request.get_json(force=True)
    SCREEN_TRACKER.set_sub_region(data['name'], data['region'])
    return jsonify({'ok': True})


@app.route('/api/st_capture_once', methods=['POST'])
def api_st_capture_once():
    result = SCREEN_TRACKER.capture_once()
    return jsonify(result)


_st_prev_odds = {}
_st_prev_odds_lock = threading.Lock()


def _screen_tracker_on_update(state):
    """Server-side callback: cancel orders instantly when odds go null (locked)."""
    global _st_prev_odds
    odds = state.get('odds', {})
    with _st_prev_odds_lock:
        for key in list(_st_prev_odds.keys()):
            if key.endswith('_home'):
                base = key[:-5]
                prev_h = _st_prev_odds.get(base + '_home')
                prev_a = _st_prev_odds.get(base + '_away')
                curr_h = odds.get(base + '_home')
                curr_a = odds.get(base + '_away')
                if prev_h is not None and prev_a is not None:
                    if curr_h is None or curr_a is None:
                        locked = 'BOTH' if curr_h is None and curr_a is None else ('HOME' if curr_h is None else 'AWAY')
                        print(f"[SCREEN-LOCK] {base} LOCKED ({locked}) — server-side emergency cancel")
                        _emergency_cancel_all()
                        break
        _st_prev_odds = dict(odds)


def _emergency_cancel_all():
    """Cancel all tracked Kalshi orders immediately from server side."""
    def _do():
        with PLACED_ORDER_LOCK:
            oids = [o['oid'] for o in PLACED_ORDER_IDS]
        if not oids:
            return
        cancelled = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_cancel_single_order, oid): oid for oid in oids}
            for f in as_completed(futs):
                if f.result():
                    cancelled += 1
        with PLACED_ORDER_LOCK:
            cancelled_set = set(oids)
            PLACED_ORDER_IDS[:] = [o for o in PLACED_ORDER_IDS
                                   if o['oid'] not in cancelled_set]
        print(f"[SCREEN-LOCK] Cancelled {cancelled}/{len(oids)} Kalshi orders")
    threading.Thread(target=_do, daemon=True).start()


@app.route('/api/st_start', methods=['POST'])
def api_st_start():
    SCREEN_TRACKER.start(interval=0.08, on_update=_screen_tracker_on_update)
    return jsonify({'ok': True})


@app.route('/api/st_stop', methods=['POST'])
def api_st_stop():
    SCREEN_TRACKER.stop()
    return jsonify({'ok': True})


@app.route('/api/st_state')
def api_st_state():
    return jsonify(SCREEN_TRACKER.get_state())


@app.route('/api/st_history')
def api_st_history():
    return jsonify(SCREEN_TRACKER.get_history())


@app.route('/api/st_reset', methods=['POST'])
def api_st_reset():
    SCREEN_TRACKER.reset()
    return jsonify({'ok': True})


@app.route('/api/st_clear_regions', methods=['POST'])
def api_st_clear_regions():
    data = request.get_json(force=True) or {}
    game_key = data.get('game_key', '')
    if game_key:
        SCREEN_TRACKER.remove_sub_regions(game_key + '_')
    else:
        SCREEN_TRACKER.set_scoreboard(None)
        SCREEN_TRACKER.sub_regions = {}
    return jsonify({'ok': True})


@app.route('/api/st_remove_game', methods=['POST'])
def api_st_remove_game():
    data = request.get_json(force=True) or {}
    game_key = data.get('game_key', '')
    if game_key:
        SCREEN_TRACKER.remove_sub_regions(game_key + '_')
    return jsonify({'ok': True})


@app.route('/api/screen_trade', methods=['POST'])
def api_screen_trade():
    data = request.get_json(force=True)
    home = data.get('home', '')
    away = data.get('away', '')
    tickers = data.get('tickers', [])
    poly_tickers = data.get('poly_tickers', [])
    contracts = data.get('contracts', CONTRACTS)
    spread_cents = data.get('spread_cents', SPREAD_CENTS)
    trade_side = data.get('trade_side', 'both')
    mode = data.get('mode', 'ioc')

    game_key = data.get('game_key', f"{home}|{away}")
    state = SCREEN_TRACKER.get_state()
    odds = state.get('odds', {})
    home_odds = odds.get(f"{game_key}_home")
    away_odds = odds.get(f"{game_key}_away")
    if not home_odds or not away_odds:
        return jsonify({'error': 'No odds from screen tracker', 'orders': []}), 400

    client_fair = data.get('home_fair_cents')
    if client_fair is not None:
        home_fair_cents = max(1, min(99, int(client_fair)))
    else:
        home_fair_prob, away_fair_prob = power_devig(home_odds, away_odds)
        home_fair_cents = int(round(home_fair_prob * 100))
        home_fair_cents = max(1, min(99, home_fair_cents))

    print(f"[SCREEN-TRADE] {game_key} mode={mode} odds={home_odds}/{away_odds} -> "
          f"fair={home_fair_cents}c/{100 - home_fair_cents}c spread={spread_cents}c side={trade_side}")

    if KALSHI_WS and tickers:
        KALSHI_WS.subscribe([t['ticker'] for t in tickers if 'ticker' in t])

    results = []

    if mode == 'maker':
        if tickers:
            ticker_names = [t['ticker'] for t in tickers]
            cancel_all_live_orders(tickers=ticker_names)
            results.extend(post_live_orders(
                tickers, home_fair_cents, contracts, spread_cents,
                home, away, trade_side))
    else:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = []
            if tickers:
                futures.append(pool.submit(post_ioc_orders,
                    tickers, home_fair_cents, contracts, spread_cents,
                    home, away, trade_side))
            if poly_tickers:
                futures.append(pool.submit(_post_poly_live_orders,
                    poly_tickers, home_fair_cents, home, away,
                    contracts, spread_cents, True, trade_side))
            for f in as_completed(futures):
                results.extend(f.result())

    return jsonify({
        'home_odds': home_odds,
        'away_odds': away_odds,
        'home_fair': home_fair_cents,
        'away_fair': 100 - home_fair_cents,
        'orders': results,
    })


# ── Main ─────────────────────────────────────────────────────────────

def main():
    global API_KEY_ID, PRIVATE_KEY, DRY_RUN, CONTRACTS, SPREAD_CENTS, POLY_CLIENT, KALSHI_WS

    parser = argparse.ArgumentParser(description="CS2 Live Trader")
    parser.add_argument('--api-key-id', type=str, default=None)
    parser.add_argument('--private-key-path', type=str, default=None)
    parser.add_argument('--poly-key-path', type=str, default=None)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--contracts', type=int, default=50)
    parser.add_argument('--spread', type=int, default=6)
    parser.add_argument('--port', type=int, default=5052)
    args = parser.parse_args()

    DRY_RUN = args.dry_run
    CONTRACTS = args.contracts
    SPREAD_CENTS = args.spread

    if not DRY_RUN:
        if not args.api_key_id or not args.private_key_path:
            print("Error: need --api-key-id and --private-key-path (or use --dry-run)")
            sys.exit(1)
        API_KEY_ID = args.api_key_id
        PRIVATE_KEY = load_private_key(args.private_key_path)

        KALSHI_WS = KalshiWS(API_KEY_ID, PRIVATE_KEY)
        KALSHI_WS.start(on_fill=_ws_fill_handler)
    else:
        API_KEY_ID = 'dry-run'

    if args.poly_key_path:
        POLY_CLIENT = PolyClient(args.poly_key_path)
        POLY_CLIENT.start_heartbeat()

    global HLTV_SCRAPER
    HLTV_SCRAPER = HLTVScoreboard()

    ws_status = 'connected' if KALSHI_WS else 'off (dry-run)'
    print(f"\n  CS2 Live Trader")
    print(f"  Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"  Contracts: {CONTRACTS} | Spread: {SPREAD_CENTS}c")
    print(f"  Polymarket: {'connected' if POLY_CLIENT else 'off'}")
    print(f"  Kalshi WS: {ws_status}")
    print(f"  http://localhost:{args.port}\n")

    app.run(host='0.0.0.0', port=args.port, debug=False)


if __name__ == '__main__':
    main()