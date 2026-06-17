"""
kalshi_edge.py — Scan Kalshi CS2 moneyline markets for edge + house mode.

Series tickers:
  KXCS2GAME — match winner ("Will [Team] win the [Away] vs. [Home] CS2 match?")
  KXCS2MAP  — map winner ("Will [Team] win map N in the [Away] vs. [Home] match?")

Usage:
  python3 kalshi_edge.py scan                     # scan for edge (no auth)
  python3 kalshi_edge.py top                      # top 25 edges
  python3 kalshi_edge.py positions --api-key-id KEY --private-key-path PEM
  python3 kalshi_edge.py trade --dry-run --api-key-id KEY --private-key-path PEM
  python3 kalshi_edge.py limit --dry-run --api-key-id KEY --private-key-path PEM

  # House mode loop (every 10 min):
  while python3 kalshi_edge.py limit --spread 4 --contracts 150 --max-contracts 600 --max-position 600 \\
    --api-key-id KEY --private-key-path PEM; do sleep 600; done
"""
import os, sys, argparse, json, uuid, re, warnings, time, random
import numpy as np
import pandas as pd
import pickle
import requests
import datetime

warnings.filterwarnings('ignore')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
MODEL_DIR = os.path.join(DATA_DIR, 'model')
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXCS2GAME"
MAP_SERIES_TICKER = "KXCS2MAP"
TOTALMAPS_SERIES_TICKER = "KXCS2TOTALMAPS"
ALL_SERIES_TICKERS = [SERIES_TICKER, MAP_SERIES_TICKER, TOTALMAPS_SERIES_TICKER]
MAX_PER_GAME = 100
MAX_PER_MARKET_TYPE = 100
MIN_BOOK_DEPTH_CHOICES = [25, 50, 75]
PREGAME_CUTOFF_MINS = 15

CONFIDENT_MATCHES = 30  # sample size at which model uncertainty is "normal"


def sample_size_spread_multiplier(home_count, away_count):
    """Widen spread for low-sample-size games. Returns multiplier >= 1.0.

    Based on 1/sqrt(n): uncertainty in win-prob estimates scales this way,
    so we scale our spread to compensate."""
    min_matches = min(home_count, away_count)
    if min_matches >= CONFIDENT_MATCHES:
        return 1.0
    import math
    return math.sqrt(CONFIDENT_MATCHES / max(min_matches, 1))

EDGE_MULTIPLIER = 1.2
LEDGER_PATH = os.path.join(DATA_DIR, 'trade_ledger.json')

import subprocess as _sp
CAP_LOG_PATH = os.path.join(DATA_DIR, 'cap_hits.log')
CAP_OVERRIDE_PATH = os.path.join(DATA_DIR, 'cap_override.json')


# ============================================================
# TRADE LEDGER
# ============================================================

def load_trade_ledger():
    if os.path.exists(LEDGER_PATH):
        try:
            with open(LEDGER_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_trade_ledger(ledger):
    try:
        with open(LEDGER_PATH, 'w') as f:
            json.dump(ledger, f)
    except Exception:
        pass


def get_edge_multiplier(ledger, game_key):
    prior = ledger.get(game_key, 0)
    return EDGE_MULTIPLIER ** prior


# ============================================================
# CAP MANAGEMENT
# ============================================================

def load_cap_overrides():
    if os.path.exists(CAP_OVERRIDE_PATH):
        try:
            with open(CAP_OVERRIDE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cap_overrides(overrides):
    with open(CAP_OVERRIDE_PATH, 'w') as f:
        json.dump(overrides, f, indent=2)


def apply_cap_overrides(args):
    overrides = load_cap_overrides()
    if not overrides:
        return
    applied = []
    if 'max_position' in overrides and hasattr(args, 'max_position'):
        old = args.max_position
        args.max_position = overrides['max_position']
        applied.append(f"max-position: {old} → {args.max_position}")
    if 'games' in overrides and overrides['games']:
        args._game_cap_overrides = overrides['games']
        for gk, cap in overrides['games'].items():
            applied.append(f"{gk.replace('|', ' vs ')}: {cap}")
    if applied:
        print(f"  [cap override] {', '.join(applied)}")


def get_game_max_position(args, game_key):
    game_overrides = getattr(args, '_game_cap_overrides', {})
    if game_key in game_overrides:
        return game_overrides[game_key]
    return args.max_position


NOTIFIED_CAPS_PATH = os.path.join(DATA_DIR, 'cap_notified.json')


def notify_max_position(game_label, existing, max_pos, command=""):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f"{game_label} — {existing}/{max_pos} contracts"
    log_line = f"[{timestamp}] [{command or 'unknown'}] {msg}\n"
    try:
        with open(CAP_LOG_PATH, 'a') as f:
            f.write(log_line)
    except Exception:
        pass
    try:
        _sp.Popen(
            ['osascript', '-e',
             f'display notification "{msg}" with title "CS2 Max Position" sound name "Glass"'],
            stdout=_sp.DEVNULL, stderr=_sp.DEVNULL
        )
    except Exception:
        pass


def print_cap_log():
    overrides = load_cap_overrides()
    if overrides:
        print(f"\n  === Active Cap Overrides ===")
        games = overrides.pop('games', {})
        for k, v in overrides.items():
            print(f"    {k}: {v}")
        for gk, cap in games.items():
            print(f"    {gk.replace('|', ' vs ')}: {cap}")
        print()
    if not os.path.exists(CAP_LOG_PATH):
        print("  No cap hits recorded yet.")
        return
    with open(CAP_LOG_PATH) as f:
        lines = f.readlines()
    if not lines:
        print("  No cap hits recorded yet.")
        return
    print(f"  === Cap Hit Log ({len(lines)} events) ===\n")
    for line in lines[-30:]:
        print(f"  {line.rstrip()}")


# ============================================================
# MODEL INTEGRATION
# ============================================================

def load_model():
    path = os.path.join(MODEL_DIR, 'model.pkl')
    if not os.path.exists(path):
        print(f"Error: {path} not found. Run python3 train_model.py first.")
        sys.exit(1)
    with open(path, 'rb') as f:
        data = pickle.load(f)
    return data['model'], data['encoders'], data.get('scale', 1.0)


def load_forfeit_model():
    path = os.path.join(MODEL_DIR, 'model.pkl')
    if not os.path.exists(path):
        return None, {}
    with open(path, 'rb') as f:
        data = pickle.load(f)
    ff_model_data = data.get('ff_model')
    model = data['model']
    encoders = data['encoders']
    team_names = encoders['team'].categories_[0]
    team_coefs = model.coef_[:len(team_names)]
    power_map = {name: float(team_coefs[i]) for i, name in enumerate(team_names)}
    return ff_model_data, power_map


def get_win_prob(model, encoders, team_a, team_b, scale=1.0):
    """Get model win probability for team_a vs team_b."""
    from train_model import predict_match, resolve_team_name
    prob = predict_match(model, encoders, team_a, team_b, scale)
    use_a = resolve_team_name(team_a, encoders) or team_a
    use_b = resolve_team_name(team_b, encoders) or team_b
    return prob, use_a, use_b


# ============================================================
# MARKET NORMALIZATION
# ============================================================

def _normalize_market(m):
    for field in ('yes_ask', 'yes_bid', 'no_ask', 'no_bid'):
        dollar_key = f'{field}_dollars'
        if dollar_key in m and field not in m:
            try:
                m[field] = int(round(float(m[dollar_key]) * 100))
            except (ValueError, TypeError):
                m[field] = 0
    for field in ('volume', 'open_interest'):
        fp_key = f'{field}_fp'
        if fp_key in m and field not in m:
            try:
                m[field] = int(round(float(m[fp_key])))
            except (ValueError, TypeError):
                m[field] = 0
    return m


# ============================================================
# TEAM NAME MATCHING
# ============================================================

def normalize_name(name):
    if not name:
        return ''
    name = name.replace('_', ' ')
    return re.sub(r'\s+', ' ', name.strip()).lower()


def match_kalshi_team(kalshi_name, our_teams):
    if not kalshi_name:
        return None
    norm = normalize_name(kalshi_name)
    # Check TEAM_ALIASES first (handles short names like "NIP" -> "Ninjas in Pyjamas")
    from train_model import TEAM_ALIASES
    alias_target = TEAM_ALIASES.get(norm)
    if alias_target:
        for t in our_teams:
            if normalize_name(t) == normalize_name(alias_target):
                return t
    # Exact match
    for t in our_teams:
        if normalize_name(t) == norm:
            return t
    # Modifiers that distinguish separate orgs (e.g. "NAVI Junior" != "Natus Vincere")
    _team_modifiers = {'junior', 'jr', 'jr.', 'academy', 'female', 'fe', 'rising'}

    def _modifier_mismatch(a_tokens, b_tokens):
        a_mods = a_tokens & _team_modifiers
        b_mods = b_tokens & _team_modifiers
        return a_mods != b_mods

    # Substring match — require at least 4 chars to avoid "THE" matching "the last resort"
    matches = []
    for t in our_teams:
        t_norm = normalize_name(t)
        if len(t_norm) >= 4 and t_norm in norm:
            matches.append(t)
        elif len(norm) >= 4 and norm in t_norm:
            matches.append(t)
    if matches:
        norm_tokens = set(norm.split())
        filtered = [t for t in matches
                    if not _modifier_mismatch(norm_tokens, set(normalize_name(t).split()))]
        if filtered:
            filtered.sort(key=lambda t: -len(normalize_name(t)))
            return filtered[0]
        matches.sort(key=lambda t: -len(normalize_name(t)))
        return matches[0]
    # Token overlap — require majority of tokens match
    norm_tokens = set(norm.split())
    for t in our_teams:
        t_words = set(normalize_name(t).split())
        if _modifier_mismatch(norm_tokens, t_words):
            continue
        overlap = sum(1 for w in t_words if w in norm.split())
        if len(t_words) > 0 and overlap >= max(1, len(t_words) // 2 + 1):
            return t
    return None


# ============================================================
# HLTV FORFEIT DETECTION
# ============================================================

def fetch_hltv_forfeits():
    """Scrape HLTV results (first page) and return set of team names that forfeited.
    A forfeit shows as best_of='def' with a 1-0 score; the loser forfeited."""
    try:
        from curl_cffi import requests as cffi_requests
        from bs4 import BeautifulSoup
    except ImportError:
        print("  [HLTV] curl_cffi/bs4 not installed — skipping forfeit check")
        return set()

    forfeited_teams = set()
    try:
        session = cffi_requests.Session()
        resp = session.get('https://www.hltv.org/results', impersonate='chrome131',
                           timeout=30, headers={'Referer': 'https://www.hltv.org/'})
        if resp.status_code != 200:
            print(f"  [HLTV] HTTP {resp.status_code}")
            return set()

        soup = BeautifulSoup(resp.text, 'html.parser')
        all_holders = soup.find_all('div', class_='results-holder')
        results_holder = all_holders[-1] if all_holders else None
        if not results_holder:
            return set()

        for con in results_holder.find_all('div', class_='result-con'):
            map_div = con.find('div', class_='map-text')
            if not map_div or map_div.get_text(strip=True).lower() != 'def':
                continue

            team1_div = con.find('div', class_='line-align team1')
            team2_div = con.find('div', class_='line-align team2')
            if not team1_div or not team2_div:
                continue
            t1_name = team1_div.find('div', class_='team')
            t2_name = team2_div.find('div', class_='team')
            if not t1_name or not t2_name:
                continue

            score_td = con.find('td', class_='result-score')
            if not score_td:
                continue
            scores = score_td.find_all('span')
            if len(scores) < 2:
                continue

            try:
                s1 = int(scores[0].get_text(strip=True))
                s2 = int(scores[1].get_text(strip=True))
            except ValueError:
                continue

            t1 = t1_name.get_text(strip=True)
            t2 = t2_name.get_text(strip=True)
            loser = t2 if s1 > s2 else t1
            forfeited_teams.add(loser.lower())
            print(f"  [HLTV] Forfeit detected: {loser} (vs {t1 if loser == t2 else t2})")

    except Exception as e:
        print(f"  [HLTV] scrape failed: {e}")

    return forfeited_teams


def fetch_hltv_finished_teams():
    """Scrape HLTV results page and return set of team name pairs that recently finished.
    Returns set of frozenset({team1_lower, team2_lower})."""
    try:
        from curl_cffi import requests as cffi_requests
        from bs4 import BeautifulSoup
    except ImportError:
        return set()

    finished = set()
    try:
        session = cffi_requests.Session()
        resp = session.get('https://www.hltv.org/results', impersonate='chrome131',
                           timeout=30, headers={'Referer': 'https://www.hltv.org/'})
        if resp.status_code != 200:
            return set()

        soup = BeautifulSoup(resp.text, 'html.parser')
        all_holders = soup.find_all('div', class_='results-holder')
        results_holder = all_holders[-1] if all_holders else None
        if not results_holder:
            return set()

        for con in results_holder.find_all('div', class_='result-con'):
            team1_div = con.find('div', class_='line-align team1')
            team2_div = con.find('div', class_='line-align team2')
            if not team1_div or not team2_div:
                continue
            t1_name = team1_div.find('div', class_='team')
            t2_name = team2_div.find('div', class_='team')
            if not t1_name or not t2_name:
                continue
            t1 = t1_name.get_text(strip=True).lower()
            t2 = t2_name.get_text(strip=True).lower()
            if t1 and t2:
                finished.add(frozenset({t1, t2}))

    except Exception:
        pass

    return finished



def is_match_finished(t1, t2, finished_pairs):
    if not finished_pairs:
        return False
    for n1 in [t1.lower(), normalize_name(t1)]:
        for n2 in [t2.lower(), normalize_name(t2)]:
            if frozenset({n1, n2}) in finished_pairs:
                return True
            for fp in finished_pairs:
                fp_list = list(fp)
                if len(fp_list) == 2:
                    if ((n1 in fp_list[0] or fp_list[0] in n1) and
                        (n2 in fp_list[1] or fp_list[1] in n2)):
                        return True
                    if ((n1 in fp_list[1] or fp_list[1] in n1) and
                        (n2 in fp_list[0] or fp_list[0] in n2)):
                        return True
    return False


LIVE_MATCHES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'live_matches.json')


def get_auto_trader_live_matches():
    """Read live matches being traded by auto_map_trader.
    Returns set of frozenset({team1_lower, team2_lower}) or empty set."""
    import json as _json
    try:
        with open(LIVE_MATCHES_FILE) as f:
            data = _json.load(f)
        updated = data.get('updated', '')
        if updated:
            from dateutil.parser import parse as _dtparse
            age = (datetime.datetime.now() - _dtparse(updated)).total_seconds()
            if age > 600:
                return set()
        pairs = set()
        for m in data.get('live', []):
            names = []
            if m.get('home'):
                names.append(m['home'].lower())
            if m.get('away'):
                names.append(m['away'].lower())
            if len(names) == 2:
                pairs.add(frozenset(names))
            names2 = []
            if m.get('team1'):
                names2.append(m['team1'].lower())
            if m.get('team2'):
                names2.append(m['team2'].lower())
            if len(names2) == 2:
                pairs.add(frozenset(names2))
        return pairs
    except (FileNotFoundError, ValueError, KeyError):
        return set()


FF_RATE_THRESHOLD = 0.05
FF_LOOKBACK_DAYS = 90
FF_MIN_MATCHES = 5

def get_forfeit_rates():
    """Compute forfeit rates from historical matches.csv data.
    Returns dict of {team_name_lower: (ff_count, total_matches, rate)}."""
    matches_file = os.path.join(DATA_DIR, 'matches.csv')
    if not os.path.exists(matches_file):
        return {}
    df = pd.read_csv(matches_file)
    if 'forfeit' not in df.columns:
        return {}
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=FF_LOOKBACK_DAYS)
    df = df[df['date'] >= cutoff]
    if df.empty:
        return {}

    team_matches = {}
    for _, row in df.iterrows():
        for t in [row['team1'], row['team2']]:
            tl = str(t).lower()
            if tl not in team_matches:
                team_matches[tl] = {'total': 0, 'ff': 0, 'name': t}
            team_matches[tl]['total'] += 1

    ff_col = df['forfeit'].fillna('').astype(str)
    for ff_team in ff_col[ff_col.str.len() > 0]:
        tl = ff_team.lower()
        if tl in team_matches:
            team_matches[tl]['ff'] += 1

    rates = {}
    for tl, info in team_matches.items():
        if info['total'] >= FF_MIN_MATCHES:
            rate = info['ff'] / info['total']
            rates[tl] = (info['ff'], info['total'], rate, info['name'])
    return rates




# ============================================================
# MARKET PARSING
# ============================================================

def extract_teams(market):
    """Extract (away, home) team names from market title.
    Kalshi CS2 format: "Will [Team] win the [Away] vs. [Home] CS2 match?"
    or "Will [Team] win map N in the [Away] vs. [Home] match?"
    """
    title = market.get('title', '') or ''
    m = re.search(r'the\s+(.+?)\s+vs\.?\s+(.+?)\s+(?:CS2\s+)?match', title, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    for text in [market.get('subtitle', ''), title]:
        if not text:
            continue
        text = re.sub(r'\s*(Map\s*\d\s+Winner|Winner|Match Winner)\s*\??\s*$', '', text,
                      flags=re.IGNORECASE).strip()
        m2 = re.match(r'^(.+?)\s+(?:at|vs\.?)\s+(.+?)$', text, re.IGNORECASE)
        if m2:
            return m2.group(1).strip(), m2.group(2).strip()
    return None, None


def detect_market_type(market):
    ticker = market.get('ticker', '')
    if ticker.startswith(TOTALMAPS_SERIES_TICKER):
        return 'totalmaps'
    if ticker.startswith(MAP_SERIES_TICKER):
        parts = ticker.split('-')
        for p in parts:
            if p in ('1', '2', '3'):
                return f'map{p}'
    for text in [market.get('title', ''), market.get('subtitle', '')]:
        if not text:
            continue
        m = re.search(r'map\s*(\d)', text.lower())
        if m:
            return f'map{m.group(1)}'
    return 'moneyline'


def determine_yes_team(market, home_team, away_team):
    """Determine if YES = home team wins.
    "Will [Team] win ..." — YES = the team named."""
    title = market.get('title', '') or ''
    m = re.match(r'^Will\s+(.+?)\s+win\b', title, re.IGNORECASE)
    if m:
        yes_team_name = m.group(1).strip()
        yes_matched = match_kalshi_team(yes_team_name, [home_team, away_team])
        if yes_matched == home_team:
            return True
        if yes_matched == away_team:
            return False
    for text in [market.get('yes_sub_title', ''), market.get('subtitle', ''), title]:
        if not text:
            continue
        norm = normalize_name(text)
        if normalize_name(home_team) in norm:
            return True
        if normalize_name(away_team) in norm:
            return False
    return None


# ============================================================
# MATCH START TIME
# ============================================================

def get_match_start_utc(market):
    """Extract match start time (UTC) from ticker or expected_expiration_time.
    Ticker format: KXCS2GAME-26MAY061000G1AAB-G1
                              ^^^^^^^^^^
                              YY MON DD HHMM (UTC)
    Fallback: expected_expiration_time - 2 hours."""
    from zoneinfo import ZoneInfo
    ticker = market.get('ticker', '')
    # Parse from ticker: after first dash, format is YYMMMDDHHMMTEAMS
    parts = ticker.split('-')
    if len(parts) >= 2:
        seg = parts[1]
        m = re.match(r'(\d{2})([A-Z]{3})(\d{2})(\d{4})', seg)
        if m:
            yy, mon, dd, hhmm = m.group(1), m.group(2), m.group(3), m.group(4)
            try:
                dt_str = f"20{yy}-{mon}-{dd} {hhmm[:2]}:{hhmm[2:]}"
                # Ticker times are ET (Eastern)
                et = ZoneInfo('America/New_York')
                local_dt = datetime.datetime.strptime(dt_str, "%Y-%b-%d %H:%M")
                utc_dt = local_dt.replace(tzinfo=et).astimezone(datetime.timezone.utc)
                return utc_dt
            except (ValueError, TypeError):
                pass

    # Fallback: expected_expiration_time - 2 hours
    exp_str = market.get('expected_expiration_time', '')
    if exp_str:
        try:
            exp_dt = datetime.datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
            return exp_dt - datetime.timedelta(hours=2)
        except (ValueError, TypeError):
            pass
    return None


def minutes_until_start(market):
    """Minutes until match start. Negative if already started."""
    start = get_match_start_utc(market)
    if not start:
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    return (start - now).total_seconds() / 60


# ============================================================
# FETCH MARKETS
# ============================================================

def fetch_cs2_markets(pregame_only=True, series_filter=None, cutoff_mins=None):
    """Fetch open CS2 markets from Kalshi.
    series_filter: list of series tickers to fetch (default: all)
    cutoff_mins: skip markets starting within this many minutes (pregame cutoff)
    """
    tickers_to_fetch = series_filter or ALL_SERIES_TICKERS
    all_markets = []
    now = datetime.datetime.now(datetime.timezone.utc)
    skipped_live = 0
    skipped_imminent = 0

    for series in tickers_to_fetch:
        cursor = None
        while True:
            params = {'series_ticker': series, 'status': 'open', 'limit': 200}
            if cursor:
                params['cursor'] = cursor
            try:
                resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=10)
            except Exception as e:
                print(f"  API error ({series}): {e}")
                break
            if resp.status_code != 200:
                print(f"  API error ({series}): {resp.status_code}")
                break
            data = resp.json()
            batch = data.get('markets', [])
            for m in batch:
                if m.get('status', '').lower() in ('closed', 'settled', 'finalized'):
                    continue
                if pregame_only:
                    exp_str = m.get('expected_expiration_time', '')
                    if exp_str:
                        try:
                            exp_dt = datetime.datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
                            hours_to_exp = (exp_dt - now).total_seconds() / 3600
                            if hours_to_exp <= 0:
                                skipped_live += 1
                                continue
                        except (ValueError, TypeError):
                            pass
                if cutoff_mins is not None:
                    mins = minutes_until_start(m)
                    if mins is not None and mins <= cutoff_mins:
                        skipped_imminent += 1
                        continue
                nm = _normalize_market(m)
                nm['market_type'] = detect_market_type(m)
                all_markets.append(nm)
            cursor = data.get('cursor', '')
            if not cursor or not batch:
                break

    type_counts = {}
    for m in all_markets:
        mt = m.get('market_type', 'moneyline')
        type_counts[mt] = type_counts.get(mt, 0) + 1
    type_str = ', '.join(f"{v} {k}" for k, v in sorted(type_counts.items()))
    print(f"  Markets: {type_str or '0 total'}")
    if skipped_live:
        print(f"  Skipped {skipped_live} expired/live markets")
    if skipped_imminent:
        print(f"  Skipped {skipped_imminent} markets starting within {cutoff_mins} min")
    return all_markets


# ============================================================
# OVER/UNDER 2.5 MAPS PROJECTION
# ============================================================

def series_prob_to_map_prob(p_series):
    """Convert P(team A wins BO3 series) to P(A wins a single map).
    Solves p^2*(3-2p) = p_series via binary search."""
    if p_series <= 0.01:
        return 0.01
    if p_series >= 0.99:
        return 0.99
    lo, hi = 0.0, 1.0
    for _ in range(64):
        mid = (lo + hi) / 2
        if mid * mid * (3 - 2 * mid) < p_series:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def prob_under_2_5(p_series):
    """P(match ends in 2 maps) given P(team A wins BO3 series).
    Under 2.5 = either team wins 2-0 = p^2 + (1-p)^2."""
    p = series_prob_to_map_prob(p_series)
    return p * p + (1 - p) * (1 - p)


def adjust_prob_for_format(prob_bo3, bo):
    """Convert model's BO3 series probability to the correct format.
    Model outputs P(win BO3). For BO1/BO5, derive map prob then recompute."""
    if bo == 3:
        return prob_bo3
    p_map = series_prob_to_map_prob(prob_bo3)
    if bo == 1:
        return p_map
    if bo == 5:
        # P(win BO5) = p^3 * (1 + 3(1-p) + 6(1-p)^2)
        q = 1 - p_map
        return p_map**3 * (1 + 3*q + 6*q*q)
    return prob_bo3


# ============================================================
# EDGE DETECTION
# ============================================================

def get_dynamic_min_edge(prob, hours_to_game=None):
    if prob >= 0.90:
        base = 0.02
    elif prob >= 0.80:
        base = 0.03
    elif prob >= 0.70:
        base = 0.04
    elif prob >= 0.60:
        base = 0.05
    elif prob >= 0.50:
        base = 0.06
    elif prob >= 0.40:
        base = 0.08
    elif prob >= 0.30:
        base = 0.10
    elif prob >= 0.20:
        base = 0.13
    elif prob >= 0.10:
        base = 0.17
    elif prob >= 0.05:
        base = 0.25
    elif prob >= 0.01:
        base = 0.40
    else:
        return 999
    return base


def find_edges(model, encoders, markets, min_edge=0.10, no_filter=False, scale=1.0):
    """Find edges between model predictions and Kalshi market prices."""
    edges = []
    matched = 0
    unmatched = []
    team_names = list(encoders['team'].categories_[0])

    for market in markets:
        away_k, home_k = extract_teams(market)
        if not away_k or not home_k:
            continue

        away_matched = match_kalshi_team(away_k, team_names)
        home_matched = match_kalshi_team(home_k, team_names)
        if not away_matched or not home_matched:
            unmatched.append(f"{away_k} vs {home_k}")
            continue

        matched += 1
        market_type = market.get('market_type', 'moneyline')

        prob_home, _, _ = get_win_prob(model, encoders, home_matched, away_matched, scale)

        yes_is_home = determine_yes_team(market, home_matched, away_matched)
        if yes_is_home is None:
            continue

        our_yes_prob = prob_home if yes_is_home else (1 - prob_home)
        our_no_prob = 1 - our_yes_prob

        yes_ask = market.get('yes_ask', 0)
        no_ask = market.get('no_ask', 0)
        if not yes_ask and not no_ask:
            continue

        yes_cost = yes_ask / 100 if yes_ask else None
        no_cost = no_ask / 100 if no_ask else None
        yes_edge = (our_yes_prob - yes_cost) / yes_cost if yes_cost else -999
        no_edge = (our_no_prob - no_cost) / no_cost if no_cost else -999

        if yes_edge >= no_edge:
            side = 'yes'
            edge = yes_edge
            prob = our_yes_prob
            cost = yes_cost
            price = yes_ask
            bet_team = home_matched if yes_is_home else away_matched
        else:
            side = 'no'
            edge = no_edge
            prob = our_no_prob
            cost = no_cost
            price = no_ask
            bet_team = away_matched if yes_is_home else home_matched

        if not no_filter and prob < 0.15:
            continue
        if not no_filter:
            dynamic_min = get_dynamic_min_edge(prob)
            if edge < dynamic_min:
                continue

        edges.append({
            'ticker': market.get('ticker', ''),
            'market_type': market_type,
            'home': home_matched,
            'away': away_matched,
            'side': side,
            'bet_team': bet_team,
            'our_prob': round(prob, 3),
            'exec_cost': round(cost, 3),
            'edge': round(edge, 3),
            'exec_price_cents': price,
            'yes_bid': market.get('yes_bid', 0),
            'yes_ask': yes_ask,
            'no_bid': market.get('no_bid', 0),
            'no_ask': no_ask,
            'volume': market.get('volume', 0),
            'home_win_prob': round(prob_home, 3),
        })

    edges.sort(key=lambda x: -x['edge'])
    seen_games = set()
    deduped = []
    for e in edges:
        game_key = '|'.join(sorted([e['home'], e['away']])) + '|' + e['market_type']
        if game_key in seen_games:
            continue
        seen_games.add(game_key)
        deduped.append(e)
    return deduped, matched, unmatched


# ============================================================
# AUTH + ORDERS
# ============================================================

def load_private_key(path):
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    with open(path, 'rb') as f:
        return load_pem_private_key(f.read(), password=None)


def make_auth_headers(private_key, api_key_id, method, path):
    from cryptography.hazmat.primitives.asymmetric import ec, padding
    from cryptography.hazmat.primitives import hashes
    import base64
    ts = str(int(datetime.datetime.now().timestamp() * 1000))
    path_clean = path.split('?')[0]
    msg = f"{ts}{method}{path_clean}".encode()
    if hasattr(private_key, 'curve'):
        sig = private_key.sign(msg, ec.ECDSA(hashes.SHA256()))
    else:
        sig = private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
    return {
        'KALSHI-ACCESS-KEY': api_key_id,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json'
    }


def fetch_orderbook_best_ask(ticker):
    """Fetch live orderbook and return {yes_best_ask, no_best_ask}."""
    try:
        url = f"{KALSHI_BASE}/markets/{ticker}/orderbook"
        resp = requests.get(url, timeout=5)
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
        return {
            'yes_best_ask': (100 - no_bids[0][0]) if no_bids else None,
            'no_best_ask': (100 - yes_bids[0][0]) if yes_bids else None,
            'yes_best_bid': yes_bids[0][0] if yes_bids else None,
            'no_best_bid': no_bids[0][0] if no_bids else None,
        }
    except Exception:
        return None


def place_limit_order(api_key_id, private_key, ticker, side, count, price_cents, dry_run=False, expiration_ts=None, client_order_id=None):
    path = '/trade-api/v2/portfolio/orders'
    url = KALSHI_BASE + '/portfolio/orders'
    yes_price = price_cents if side == 'yes' else (100 - price_cents)
    if client_order_id is None:
        client_order_id = f"mm-{uuid.uuid4()}"
    order = {
        'ticker': ticker, 'action': 'buy', 'side': side,
        'count': count, 'type': 'limit', 'yes_price': yes_price,
        'client_order_id': client_order_id,
    }
    if expiration_ts:
        order['expiration_ts'] = expiration_ts
    if dry_run:
        cost = count * price_cents / 100
        print(f"      [DRY RUN] BID {count} {side.upper()} @ {price_cents}c | cost if filled: ${cost:.2f}")
        return True
    for attempt in range(3):
        headers = make_auth_headers(private_key, api_key_id, 'POST', path)
        try:
            resp = requests.post(url, headers=headers, json=order, timeout=10)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
                continue
            print(f"      FAILED after 3 attempts: {e}")
            return False
        if resp.status_code == 201:
            oid = resp.json().get('order', {}).get('order_id', '')
            status = resp.json().get('order', {}).get('status', '?')
            print(f"      ORDER PLACED (id={oid}, status={status})")
            return oid or True
        elif resp.status_code == 429 and attempt < 2:
            time.sleep(1.0 * (attempt + 1))
            continue
        else:
            print(f"      FAILED: {resp.status_code} {resp.text[:200]}")
            return False
    return False


def place_ioc_order(api_key_id, private_key, ticker, side, count, price_cents, dry_run=False):
    path = '/trade-api/v2/portfolio/orders'
    url = KALSHI_BASE + '/portfolio/orders'
    yes_price = price_cents if side == 'yes' else (100 - price_cents)
    order = {
        'ticker': ticker, 'action': 'buy', 'side': side,
        'count': count, 'type': 'limit', 'yes_price': yes_price,
        'client_order_id': str(uuid.uuid4()),
        'time_in_force': 'immediate_or_cancel',
    }
    if dry_run:
        cost = count * price_cents / 100
        payout = count * (100 - price_cents) / 100
        print(f"    [DRY RUN] IOC BUY {count} {side.upper()} @ {price_cents}c "
              f"| risk ${cost:.2f} to win ${payout:.2f}")
        return True
    headers = make_auth_headers(private_key, api_key_id, 'POST', path)
    resp = requests.post(url, headers=headers, json=order, timeout=10)
    if resp.status_code == 201:
        order_data = resp.json().get('order', {})
        fill_count = order_data.get('fill_count', 0)
        if fill_count > 0:
            print(f"    FILLED {fill_count}/{count} @ {price_cents}c")
            return fill_count
        else:
            print(f"    IOC NOT FILLED @ {price_cents}c")
            return 0
    else:
        print(f"    FAILED: {resp.status_code} {resp.text[:200]}")
        return 0


def _cancel_order_with_retry(api_key_id, private_key, oid, max_retries=3):
    for attempt in range(max_retries):
        cancel_path = f'/trade-api/v2/portfolio/orders/{oid}'
        cancel_url = KALSHI_BASE + f'/portfolio/orders/{oid}'
        hdrs = make_auth_headers(private_key, api_key_id, 'DELETE', cancel_path)
        try:
            r = requests.delete(cancel_url, headers=hdrs, timeout=10)
            if r.status_code in (200, 204):
                return True
            if r.status_code == 429:
                time.sleep(1.0 * (attempt + 1))
                continue
            return False
        except Exception:
            return False
    return False


def cancel_orders_for_ticker(api_key_id, private_key, ticker):
    path = '/trade-api/v2/portfolio/orders'
    url = KALSHI_BASE + '/portfolio/orders'
    params = {'status': 'resting', 'ticker': ticker}
    headers = make_auth_headers(private_key, api_key_id, 'GET', path)
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            return 0
        orders = resp.json().get('orders', [])
        cancelled = 0
        for order in orders:
            oid = order.get('order_id', '')
            if _cancel_order_with_retry(api_key_id, private_key, oid):
                cancelled += 1
            time.sleep(0.1)
        return cancelled
    except Exception:
        return 0


def cancel_stale_mm_orders(api_key_id, private_key):
    path = '/trade-api/v2/portfolio/orders'
    url = KALSHI_BASE + '/portfolio/orders'
    params = {'status': 'resting', 'limit': 1000}
    hdrs = make_auth_headers(private_key, api_key_id, 'GET', path)
    try:
        resp = requests.get(url, headers=hdrs, params=params, timeout=10)
        if resp.status_code != 200:
            return
        orders = resp.json().get('orders', [])
        cs2_orders = [o for o in orders
                      if any(o.get('ticker', '').startswith(s) for s in ALL_SERIES_TICKERS)
                      and (o.get('client_order_id', '') or '').startswith('mm-')]
        if not cs2_orders:
            return
        cancelled = 0
        for o in cs2_orders:
            if _cancel_order_with_retry(api_key_id, private_key, o.get('order_id', '')):
                cancelled += 1
            time.sleep(0.1)
        print(f"  Cancelled {cancelled}/{len(cs2_orders)} house-mode resting orders")
    except Exception as e:
        print(f"  Error cancelling stale orders: {e}")


# ============================================================
# POSITIONS
# ============================================================

def get_live_positions(api_key_id, private_key, markets=None):
    """Fetch live CS2 positions grouped by event prefix.
    Returns {event_prefix: total_contracts, ticker: pos} or (None, None).
    Event prefix = ticker minus last segment (e.g., KXCS2GAME-26MAY061000G1AAB)."""
    path = '/trade-api/v2/portfolio/positions'
    url = KALSHI_BASE + '/portfolio/positions'
    headers = make_auth_headers(private_key, api_key_id, 'GET', path)
    all_positions = []
    cursor = None
    while True:
        try:
            params = {'settlement_status': 'unsettled', 'limit': 200}
            if cursor:
                params['cursor'] = cursor
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code != 200:
                return None, None
            data = resp.json()
            batch = data.get('market_positions', [])
            all_positions.extend(batch)
            cursor = data.get('cursor', '')
            if not cursor or not batch:
                break
        except Exception:
            return None, None

    event_positions = {}
    ticker_positions = {}
    for p in all_positions:
        t = p.get('ticker', '')
        raw_pos = float(p.get('position_fp', p.get('position', 0)))
        pos = int(abs(raw_pos))
        if not t or pos == 0:
            continue
        if not any(t.upper().startswith(s.upper()) for s in ALL_SERIES_TICKERS):
            continue
        # Group by event prefix (strip team suffix after last dash)
        event_key = '-'.join(t.split('-')[:-1]) if '-' in t else t
        event_positions[event_key] = event_positions.get(event_key, 0) + pos
        ticker_positions[t] = ticker_positions.get(t, 0) + pos
    return event_positions, ticker_positions



# ============================================================
# HOUSE MODE (limit_cmd)
# ============================================================

def limit_cmd(args):
    """House mode: post resting bids on both sides with model-based pricing."""
    apply_cap_overrides(args)
    MAX_POS = args.max_position

    model, encoders, scale = load_model()
    team_names = list(encoders['team'].categories_[0])

    # Load match counts per team for minimum-matches filter
    from train_model import load_matches, build_training_data
    _df_matches = load_matches()
    _train_df = build_training_data(_df_matches)
    team_match_counts = _train_df['team'].value_counts().to_dict()
    MIN_TEAM_MATCHES = 25

    private_key = None
    if not args.dry_run:
        if not args.api_key_id or not args.private_key_path:
            print("Need --api-key-id and --private-key-path")
            sys.exit(1)
        private_key = load_private_key(args.private_key_path)

    # Polymarket setup
    poly_client = None
    poly_contracts = getattr(args, 'poly_contracts', None) or args.contracts
    poly_max_contracts = args.max_contracts // 2
    poly_key_path = getattr(args, 'poly_key_path', None)
    if poly_key_path:
        from poly_cs2 import (PolyClient, fetch_poly_cs2_markets, get_poly_cs2_positions,
                               build_poly_market_lookup, get_poly_orderbook, get_best_ask as poly_best_ask)
        if not args.dry_run:
            poly_client = PolyClient(poly_key_path)
            poly_client.start_heartbeat()
        print(f"  [Poly] Enabled — ${poly_contracts}/side, max {poly_max_contracts}/game")

    prev_round_orders = {}
    seen_fills = set()
    RETRAIN_EVERY = 24  # rounds (24 * 10min = 4 hours)

    round_num = 0
    while True:
        round_num += 1

        if round_num > 1 and round_num % RETRAIN_EVERY == 1:
            print(f"\n  [RETRAIN] Scraping HLTV + retraining model...")
            try:
                _rc = _sp.run(
                    [sys.executable, 'scrape_hltv.py'],
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                    timeout=300, capture_output=True, text=True,
                )
                if _rc.returncode == 0:
                    print(f"  [RETRAIN] Scrape OK")
                else:
                    print(f"  [RETRAIN] Scrape failed (rc={_rc.returncode}): {_rc.stderr[:200]}")
            except Exception as e:
                print(f"  [RETRAIN] Scrape error: {e}")
            try:
                _rc = _sp.run(
                    [sys.executable, 'train_model.py'],
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                    timeout=300, capture_output=True, text=True,
                )
                if _rc.returncode == 0:
                    print(f"  [RETRAIN] Train OK — reloading model")
                    model, encoders, scale = load_model()
                    team_names = list(encoders['team'].categories_[0])
                    from train_model import load_matches, build_training_data
                    _df_matches = load_matches()
                    _train_df = build_training_data(_df_matches)
                    team_match_counts = _train_df['team'].value_counts().to_dict()
                else:
                    print(f"  [RETRAIN] Train failed (rc={_rc.returncode}): {_rc.stderr[:200]}")
            except Exception as e:
                print(f"  [RETRAIN] Train error: {e}")

        now = datetime.datetime.now(datetime.timezone.utc)
        mode = "DRY RUN" if args.dry_run else "LIVE"
        print(f"\n{'='*60}")
        print(f"  [HOUSE MODE - {mode}] Round {round_num} | "
              f"{now.strftime('%H:%M:%S')}")
        bo_label = f" | BO{args.best_of}" if args.best_of != 3 else ""
        print(f"  {args.contracts}/side | max {MAX_POS}/game | spread {args.spread}c{bo_label}")
        print(f"{'='*60}")

        if prev_round_orders and private_key and not args.dry_run:
            try:
                _path = '/trade-api/v2/portfolio/fills'
                _url = KALSHI_BASE + '/portfolio/fills'
                _hdrs = make_auth_headers(private_key, args.api_key_id, 'GET', _path)
                _resp = requests.get(_url, headers=_hdrs,
                                     params={'limit': 1000}, timeout=10)
                if _resp.status_code == 200:
                    fill_totals = {}
                    for f in _resp.json().get('fills', []):
                        oid = f.get('order_id', '')
                        if oid in prev_round_orders and oid not in seen_fills:
                            fill_totals[oid] = fill_totals.get(oid, 0) + float(f.get('count_fp', 0))
                    for oid, filled_qty in fill_totals.items():
                        info = prev_round_orders[oid]
                        if filled_qty >= info['count']:
                            seen_fills.add(oid)
                            print(f"  [FULL FILL] {info['side'].upper()} {filled_qty:.0f}/{info['count']} on {info['event_key']}")
            except Exception:
                pass

        if private_key and not args.dry_run:
            cancel_stale_mm_orders(args.api_key_id, private_key)

        try:
            markets = fetch_cs2_markets(pregame_only=True,
                                        series_filter=[SERIES_TICKER, TOTALMAPS_SERIES_TICKER],
                                        cutoff_mins=PREGAME_CUTOFF_MINS)
        except Exception as e:
            print(f"Error fetching markets: {e}")
            if args.dry_run:
                break
            time.sleep(60)
            continue

        if not markets:
            print("  No markets found.")
            if args.dry_run:
                break
            time.sleep(300)
            continue

        event_positions = {}
        ticker_positions = {}
        if private_key and not args.dry_run:
            gp, tp = get_live_positions(args.api_key_id, private_key, markets)
            if gp is None:
                print("  WARNING: could not fetch positions — skipping round")
                if args.dry_run:
                    break
                time.sleep(60)
                continue
            event_positions = gp
            ticker_positions = tp or {}
            total_held = sum(event_positions.values())
            if total_held > 0:
                print(f"  Kalshi positions: {total_held} contracts across {len(event_positions)} events")
                for ek, held in sorted(event_positions.items(), key=lambda x: -x[1]):
                    if held > 0:
                        print(f"    {ek}: {held}")

        # Polymarket: fetch markets, positions, build lookup
        poly_markets = []
        poly_market_lookup = {}
        poly_positions = {}
        if poly_key_path:
            poly_markets = fetch_poly_cs2_markets(pregame_only=True, today_only=False, our_teams=team_names)
            poly_market_lookup = build_poly_market_lookup(poly_markets)
            if poly_client or args.dry_run:
                poly_positions = get_poly_cs2_positions(poly_client, team_names) if poly_client else {}
            poly_total = sum(poly_positions.values())
            print(f"  Poly markets: {len(poly_markets)} | Poly positions: {poly_total:.0f} shares across {len(poly_positions)} games")
            if poly_positions:
                for gk, held in sorted(poly_positions.items(), key=lambda x: -x[1]):
                    print(f"    {gk.replace('|', ' vs ')}: {held:.0f}")

        ledger = load_trade_ledger()
        total_orders = 0
        total_pairs = 0
        this_round_orders = {}


        forfeited_teams = fetch_hltv_forfeits()
        if forfeited_teams:
            print(f"  HLTV: {len(forfeited_teams)} forfeited team(s): {', '.join(forfeited_teams)}")
        else:
            print(f"  HLTV: no recent forfeits")

        finished_pairs = fetch_hltv_finished_teams()
        if finished_pairs:
            print(f"  HLTV: {len(finished_pairs)} recently finished match(es)")

        auto_trader_live = get_auto_trader_live_matches()
        if auto_trader_live:
            print(f"  Auto-trader: {len(auto_trader_live)} live match(es) — will skip")

        ff_model_data, ff_power_map = load_forfeit_model()
        ff_all_df = None
        if ff_model_data:
            from train_model import load_matches as _load_matches_ff
            ff_all_df = _load_matches_ff(include_forfeits=True)
            print(f"  Forfeit model: loaded")
        else:
            ff_rates = get_forfeit_rates()
            high_ff = {t: v for t, v in ff_rates.items() if v[2] >= FF_RATE_THRESHOLD}
            if high_ff:
                print(f"  Forfeit risk ({FF_LOOKBACK_DAYS}d, >={FF_RATE_THRESHOLD:.0%}):")
                for t, (fc, tot, rate, name) in sorted(high_ff.items(), key=lambda x: -x[1][2]):
                    print(f"    {name}: {fc}/{tot} ({rate:.0%})")
            else:
                print(f"  No high-forfeit teams in {FF_LOOKBACK_DAYS}d window")
        print()

        game_markets = [m for m in markets if m.get('ticker', '').startswith(SERIES_TICKER)]
        totalmaps_markets = [m for m in markets if m.get('market_type') == 'totalmaps']
        if totalmaps_markets:
            print(f"  Over/under 2.5 maps: {len(totalmaps_markets)} markets")

        # Group game markets by event (strip team suffix: KXCS2GAME-26MAY061000G1AAB)
        events = {}
        for market in game_markets:
            ticker = market.get('ticker', '')
            event_key = '-'.join(ticker.split('-')[:-1]) if '-' in ticker else ticker
            if event_key not in events:
                events[event_key] = []
            events[event_key].append(market)

        for event_key, event_markets in events.items():
            # Extract teams from the first market — same match for all tickers in event
            away_k, home_k = extract_teams(event_markets[0])
            if not away_k or not home_k:
                continue

            away_matched = match_kalshi_team(away_k, team_names)
            home_matched = match_kalshi_team(home_k, team_names)
            if not away_matched or not home_matched:
                unmatched = []
                if not away_matched:
                    unmatched.append(f"away={away_k}")
                if not home_matched:
                    unmatched.append(f"home={home_k}")
                print(f"  {away_k} vs {home_k} | SKIP — unmatched team ({', '.join(unmatched)})\n")
                continue

            skip_pairs = {frozenset({'hyperspirit', 'altreides'}), frozenset({'hyperspirit', 'atreides'})}
            skip_events = {'KXCS2GAME-26JUN070700HSATR', 'KXCS2GAME-26JUN070700ATRAPOGEE'}
            if event_key.upper() in skip_events:
                print(f"  {away_k} vs {home_k} | SKIP — event blocklisted ({event_key})\n")
                continue
            pair_key = frozenset({home_matched.lower(), away_matched.lower()})
            if pair_key in skip_pairs:
                print(f"  {away_matched} vs {home_matched} | SKIP — blocklisted\n")
                continue

            if forfeited_teams:
                if home_matched.lower() in forfeited_teams or away_matched.lower() in forfeited_teams:
                    ff_team = home_matched if home_matched.lower() in forfeited_teams else away_matched
                    print(f"  {away_matched} vs {home_matched} | SKIP — {ff_team} forfeited on HLTV\n")
                    continue

            if is_match_finished(home_matched, away_matched, finished_pairs) or \
               is_match_finished(home_k, away_k, finished_pairs):
                print(f"  {away_matched} vs {home_matched} | SKIP — match finished on HLTV\n")
                continue

            if auto_trader_live and (
                frozenset({home_matched.lower(), away_matched.lower()}) in auto_trader_live or
                is_match_finished(home_matched, away_matched, auto_trader_live) or
                is_match_finished(home_k, away_k, auto_trader_live)):
                if private_key and not args.dry_run:
                    for m in event_markets:
                        cancel_orders_for_ticker(args.api_key_id, private_key, m.get('ticker', ''))
                if poly_client and poly_market_lookup:
                    gk = '|'.join(sorted([home_matched, away_matched]))
                    _pm = poly_market_lookup.get(gk)
                    if _pm:
                        poly_client.cancel_token_orders(_pm['token_a'])
                        poly_client.cancel_token_orders(_pm['token_b'])
                print(f"  {away_matched} vs {home_matched} | SKIP — live on auto-trader (cancelled)\n")
                continue

            ff_home_prob = 0.0
            ff_away_prob = 0.0
            if ff_model_data and ff_all_df is not None:
                from train_model import predict_forfeit
                ff_home_prob = predict_forfeit(ff_model_data, home_matched, away_matched, ff_power_map, ff_all_df)
                ff_away_prob = predict_forfeit(ff_model_data, away_matched, home_matched, ff_power_map, ff_all_df)
                if ff_home_prob >= FF_RATE_THRESHOLD or ff_away_prob >= FF_RATE_THRESHOLD:
                    parts = []
                    if ff_home_prob >= FF_RATE_THRESHOLD:
                        parts.append(f"{home_matched}={ff_home_prob:.1%}")
                    if ff_away_prob >= FF_RATE_THRESHOLD:
                        parts.append(f"{away_matched}={ff_away_prob:.1%}")
                    print(f"  FF risk: {', '.join(parts)} — skip risky side, boost opponent")
            elif not ff_model_data:
                for side in [home_matched, away_matched]:
                    sl = side.lower()
                    if sl in high_ff:
                        fc, tot, rate, _ = high_ff[sl]
                        if side == home_matched:
                            ff_home_prob = rate
                        else:
                            ff_away_prob = rate

            # Skip if either team has fewer than MIN_TEAM_MATCHES
            home_count = team_match_counts.get(home_matched, 0)
            away_count = team_match_counts.get(away_matched, 0)
            if home_count < MIN_TEAM_MATCHES or away_count < MIN_TEAM_MATCHES:
                low_team = home_matched if home_count < away_count else away_matched
                low_count = min(home_count, away_count)
                print(f"  {away_matched} vs {home_matched} | SKIP ({low_team} only {low_count} matches)\n")
                continue

            game_key = '|'.join(sorted([home_matched, away_matched]))

            # Detect BO format from Polymarket title (e.g. "(BO1) - IEM Cologne Major")
            match_bo = getattr(args, 'best_of', 3)
            pm = poly_market_lookup.get(game_key) if poly_market_lookup else None
            if pm:
                ptitle = pm.get('title', '').lower()
                if '(bo1)' in ptitle:
                    match_bo = 1
                elif '(bo5)' in ptitle:
                    match_bo = 5
                elif '(bo3)' in ptitle:
                    match_bo = 3

            # Compute model probability ONCE for this match
            prob_home_bo3, _, _ = get_win_prob(model, encoders, home_matched, away_matched, scale)
            prob_home = adjust_prob_for_format(prob_home_bo3, match_bo)
            prob_away = 1 - prob_home

            ff_block_home = ff_home_prob >= FF_RATE_THRESHOLD
            ff_block_away = ff_away_prob >= FF_RATE_THRESHOLD

            # Position check — combine Kalshi + Polymarket
            kalshi_held = event_positions.get(event_key, 0)
            poly_held = poly_positions.get(game_key, 0) if poly_positions else 0
            game_held = kalshi_held + int(poly_held)
            mult = get_edge_multiplier(ledger, game_key)
            mult_str = f" x{mult:.2f}" if mult > 1.0 else ""

            # Cap check (applies to whole match across both exchanges)
            max_pos = get_game_max_position(args, game_key)
            game_room = max(0, max_pos - game_held)

            matchup = f"{away_matched} vs {home_matched}"
            held_parts = []
            if kalshi_held:
                held_parts.append(f"K:{kalshi_held}")
            if poly_held:
                held_parts.append(f"P:{int(poly_held)}")
            held_str = f"held {'+'.join(held_parts)}={game_held}" if held_parts else "no pos"
            ff_str = ""
            if ff_block_home:
                ff_str += f" | FF-BLOCK {home_matched}({ff_home_prob:.0%})"
            if ff_block_away:
                ff_str += f" | FF-BLOCK {away_matched}({ff_away_prob:.0%})"
            bo_tag = f" [BO{match_bo}]" if match_bo != 3 else ""
            print(f"  {matchup} | {held_str}{mult_str} | home={prob_home:.0%} away={prob_away:.0%}{bo_tag}{ff_str}")

            if game_room == 0:
                if private_key and not args.dry_run:
                    for m in event_markets:
                        cancel_orders_for_ticker(args.api_key_id, private_key, m.get('ticker', ''))
                if poly_client and pm:
                    poly_client.cancel_token_orders(pm['token_a'])
                    poly_client.cancel_token_orders(pm['token_b'])
                print(f"    CAPPED at {max_pos}\n")
                continue

            match_start = get_match_start_utc(event_markets[0])
            match_exp_ts = int(match_start.timestamp()) if match_start else None

            for market in event_markets:
                ticker = market.get('ticker', '')

                yes_is_home = determine_yes_team(market, home_matched, away_matched)
                if yes_is_home is None:
                    continue

                our_yes_prob = prob_home if yes_is_home else prob_away
                our_no_prob = 1 - our_yes_prob
                fair_yes = int(round(our_yes_prob * 100))
                fair_no = 100 - fair_yes

                yes_team = home_matched if yes_is_home else away_matched
                no_team = away_matched if yes_is_home else home_matched

                # Fetch live orderbook to cap bids below best ask (always maker)
                ob = fetch_orderbook_best_ask(ticker)
                mkt_yes_ask = ob['yes_best_ask'] if ob and ob.get('yes_best_ask') else 0
                mkt_no_ask = ob['no_best_ask'] if ob and ob.get('no_best_ask') else 0
                mkt_yes_bid = ob.get('yes_best_bid', 0) or 0 if ob else 0
                mkt_no_bid = ob.get('no_best_bid', 0) or 0 if ob else 0

                # Widen spread for thin books (wide bid-ask gap or no book)
                yes_gap = (mkt_yes_ask - mkt_yes_bid) if (mkt_yes_ask and mkt_yes_bid) else 99
                no_gap = (mkt_no_ask - mkt_no_bid) if (mkt_no_ask and mkt_no_bid) else 99
                wide_book = yes_gap > 10 or no_gap > 10
                base_spread = args.spread * 2 if wide_book else args.spread
                ss_mult = sample_size_spread_multiplier(home_count, away_count)
                spread_cents = int(round(base_spread * ss_mult))

                yes_target = max(1, fair_yes - spread_cents)
                no_target = max(1, fair_no - spread_cents)
                yes_bid = min(yes_target, mkt_yes_ask - 1) if mkt_yes_ask > 0 else yes_target
                no_bid = min(no_target, mkt_no_ask - 1) if mkt_no_ask > 0 else no_target

                yes_bid = max(1, yes_bid)
                no_bid = max(1, no_bid)

                skip_yes = yes_bid <= 2 or (fair_yes - spread_cents) < 1
                skip_no = no_bid <= 2 or (fair_no - spread_cents) < 1

                if yes_is_home and ff_block_home:
                    skip_yes = True
                elif not yes_is_home and ff_block_away:
                    skip_yes = True
                if yes_is_home and ff_block_away:
                    skip_no = True
                elif not yes_is_home and ff_block_home:
                    skip_no = True

                yes_contracts = min(args.contracts, args.max_contracts, game_room) if not skip_yes else 0
                no_contracts = min(args.contracts, args.max_contracts, game_room) if not skip_no else 0

                yes_edge = (our_yes_prob - yes_bid / 100) / (yes_bid / 100) if yes_bid > 0 else 0
                no_edge = (our_no_prob - no_bid / 100) / (no_bid / 100) if no_bid > 0 else 0

                ticker_short = ticker.split('-')[-1]
                ask_info = f" | ask: YES={mkt_yes_ask}c NO={mkt_no_ask}c" if (mkt_yes_ask or mkt_no_ask) else ""
                print(f"    [{ticker_short}] Fair: YES={fair_yes}c({yes_team}) NO={fair_no}c({no_team}){ask_info}")
                spread_tag = " [WIDE]" if wide_book else ""
                if ss_mult > 1.0:
                    spread_tag += f" [SS x{ss_mult:.2f} n={min(home_count, away_count)}]"
                if not skip_yes:
                    cap_reason = " [ASK CAP]" if yes_bid < yes_target else ""
                    print(f"      BID {yes_contracts} YES @ {yes_bid}c edge={yes_edge:+.1%}{cap_reason}{spread_tag}")
                if not skip_no:
                    cap_reason = " [ASK CAP]" if no_bid < no_target else ""
                    print(f"      BID {no_contracts} NO @ {no_bid}c edge={no_edge:+.1%}{cap_reason}{spread_tag}")
                if skip_yes and skip_no:
                    print(f"      SKIP — bids too low")

                yes_ok = False
                no_ok = False
                if not skip_yes:
                    yes_ok = place_limit_order(args.api_key_id, private_key, ticker, 'yes',
                                              yes_contracts, yes_bid, args.dry_run,
                                              expiration_ts=match_exp_ts)
                    if yes_ok and isinstance(yes_ok, str):
                        this_round_orders[yes_ok] = {'event_key': event_key, 'side': 'yes', 'count': yes_contracts,
                                                     'ticker': ticker, 'price': yes_bid}
                if not skip_no:
                    no_ok = place_limit_order(args.api_key_id, private_key, ticker, 'no',
                                             no_contracts, no_bid, args.dry_run,
                                             expiration_ts=match_exp_ts)
                    if no_ok and isinstance(no_ok, str):
                        this_round_orders[no_ok] = {'event_key': event_key, 'side': 'no', 'count': no_contracts,
                                                     'ticker': ticker, 'price': no_bid}
                if yes_ok:
                    total_orders += 1
                if no_ok:
                    total_orders += 1
                if yes_ok and no_ok:
                    total_pairs += 1

            # --- Polymarket orders for this game ---
            if pm:
                gs = pm.get('game_start_time')
                if gs and (gs - now).total_seconds() < 1200:
                    if poly_client:
                        poly_client.cancel_token_orders(pm['token_a'])
                        poly_client.cancel_token_orders(pm['token_b'])
                    mins_left = max(0, (gs - now).total_seconds() / 60)
                    print(f"    [POLY] {pm['team_a']} vs {pm['team_b']} starts in {mins_left:.0f}m — cancelled")
                    pm = None
            if pm and pm.get('accepting_orders') and game_room > 0:
                poly_spread_dec = (args.spread * ss_mult) / 100
                poly_fair_a = round(prob_home, 2)
                poly_fair_b = round(prob_away, 2)

                # Determine which poly outcome is home
                poly_home = match_kalshi_team(pm['team_a'], team_names)
                if poly_home == home_matched:
                    raw_fair_a, raw_fair_b = poly_fair_a, poly_fair_b
                    poly_bid_a = round(max(0.01, raw_fair_a - poly_spread_dec), 2)
                    poly_bid_b = round(max(0.01, raw_fair_b - poly_spread_dec), 2)
                else:
                    raw_fair_a, raw_fair_b = poly_fair_b, poly_fair_a
                    poly_bid_a = round(max(0.01, raw_fair_a - poly_spread_dec), 2)
                    poly_bid_b = round(max(0.01, raw_fair_b - poly_spread_dec), 2)

                ob_a = get_poly_orderbook(pm['token_a'])
                ob_b = get_poly_orderbook(pm['token_b'])
                ask_a = poly_best_ask(ob_a)
                ask_b = poly_best_ask(ob_b)
                tick = float(pm['tick_size'])

                if ask_a > 0:
                    poly_bid_a = min(poly_bid_a, round(ask_a - tick, 2))
                if ask_b > 0:
                    poly_bid_b = min(poly_bid_b, round(ask_b - tick, 2))

                poly_bid_a = max(0.01, poly_bid_a)
                poly_bid_b = max(0.01, poly_bid_b)

                poly_room = max(0, poly_max_contracts - int(poly_held))
                poly_size = min(poly_contracts, poly_room, game_room)
                skip_a = poly_size < 5 or poly_bid_a <= 0.02 or (raw_fair_a - poly_spread_dec) < 0.01 or (ff_block_home if poly_home == home_matched else ff_block_away)
                skip_b = poly_size < 5 or poly_bid_b <= 0.02 or (raw_fair_b - poly_spread_dec) < 0.01 or (ff_block_away if poly_home == home_matched else ff_block_home)

                print(f"    [POLY] {pm['team_a']}={poly_bid_a:.2f} (ask={ask_a:.2f}) | "
                      f"{pm['team_b']}={poly_bid_b:.2f} (ask={ask_b:.2f}) | ${poly_size}/side")

                if poly_client or args.dry_run:
                    if not skip_a:
                        if poly_client:
                            poly_client.cancel_token_orders(pm['token_a'])
                        resp = poly_client.place_order(
                            pm['token_a'], "BUY", poly_bid_a, poly_size,
                            tick_size=pm['tick_size'], neg_risk=pm['neg_risk'],
                            order_type="GTC", dry_run=args.dry_run,
                        ) if poly_client else None
                        if args.dry_run and not poly_client:
                            shares = poly_size / poly_bid_a if poly_bid_a > 0 else 0
                            print(f"      [DRY RUN] POLY BUY ${poly_size} {pm['team_a']} @ {poly_bid_a:.2f} ({shares:.0f} shares)")
                        if resp or args.dry_run:
                            total_orders += 1
                    if not skip_b:
                        if poly_client:
                            poly_client.cancel_token_orders(pm['token_b'])
                        resp = poly_client.place_order(
                            pm['token_b'], "BUY", poly_bid_b, poly_size,
                            tick_size=pm['tick_size'], neg_risk=pm['neg_risk'],
                            order_type="GTC", dry_run=args.dry_run,
                        ) if poly_client else None
                        if args.dry_run and not poly_client:
                            shares = poly_size / poly_bid_b if poly_bid_b > 0 else 0
                            print(f"      [DRY RUN] POLY BUY ${poly_size} {pm['team_b']} @ {poly_bid_b:.2f} ({shares:.0f} shares)")
                        if resp or args.dry_run:
                            total_orders += 1

            print()

        # --- Poly-only games (not on Kalshi) ---
        if poly_market_lookup:
            kalshi_game_keys = set()
            for event_key_k, event_markets_k in events.items():
                away_k2, home_k2 = extract_teams(event_markets_k[0])
                a_m = match_kalshi_team(away_k2, team_names) if away_k2 else None
                h_m = match_kalshi_team(home_k2, team_names) if home_k2 else None
                if a_m and h_m:
                    kalshi_game_keys.add('|'.join(sorted([a_m, h_m])))

            poly_only = {gk: pm for gk, pm in poly_market_lookup.items() if gk not in kalshi_game_keys}
            if poly_only:
                print(f"  --- Poly-only games ({len(poly_only)}) ---")
                for gk, pm in poly_only.items():
                    teams = gk.split('|')
                    if len(teams) != 2:
                        continue

                    if auto_trader_live and (
                        frozenset({teams[0].lower(), teams[1].lower()}) in auto_trader_live):
                        if poly_client:
                            poly_client.cancel_token_orders(pm['token_a'])
                            poly_client.cancel_token_orders(pm['token_b'])
                        print(f"  {pm['team_a']} vs {pm['team_b']} | SKIP — live on auto-trader (cancelled)")
                        continue

                    gs = pm.get('game_start_time')
                    if gs and (gs - now).total_seconds() < 1200:
                        if poly_client:
                            poly_client.cancel_token_orders(pm['token_a'])
                            poly_client.cancel_token_orders(pm['token_b'])
                        mins_left = max(0, (gs - now).total_seconds() / 60)
                        print(f"  {pm['team_a']} vs {pm['team_b']} starts in {mins_left:.0f}m — cancelled")
                        continue

                    t_a, t_b = teams
                    home_count = team_match_counts.get(t_a, 0)
                    away_count = team_match_counts.get(t_b, 0)
                    if home_count < MIN_TEAM_MATCHES or away_count < MIN_TEAM_MATCHES:
                        continue

                    poly_held_g = int(poly_positions.get(gk, 0))
                    max_pos_g = get_game_max_position(args, gk)
                    room_g = max(0, max_pos_g - poly_held_g)
                    if room_g <= 0:
                        if poly_client:
                            poly_client.cancel_token_orders(pm['token_a'])
                            poly_client.cancel_token_orders(pm['token_b'])
                        print(f"  {gk.replace('|', ' vs ')} | CAPPED at {max_pos_g}")
                        continue

                    try:
                        prob_a, _, _ = get_win_prob(model, encoders, t_a, t_b, scale)
                    except Exception:
                        continue
                    prob_b = 1 - prob_a

                    poly_home = match_kalshi_team(pm['team_a'], team_names)
                    if poly_home == t_a:
                        fair_a, fair_b = prob_a, prob_b
                    else:
                        fair_a, fair_b = prob_b, prob_a

                    ss_mult_po = sample_size_spread_multiplier(home_count, away_count)
                    poly_spread_dec = (args.spread * ss_mult_po) / 100
                    skip_po_a = (fair_a - poly_spread_dec) < 0.01
                    skip_po_b = (fair_b - poly_spread_dec) < 0.01
                    bid_a = round(max(0.01, fair_a - poly_spread_dec), 2)
                    bid_b = round(max(0.01, fair_b - poly_spread_dec), 2)

                    ob_a = get_poly_orderbook(pm['token_a'])
                    ob_b = get_poly_orderbook(pm['token_b'])
                    ask_a = poly_best_ask(ob_a)
                    ask_b = poly_best_ask(ob_b)
                    tick = float(pm['tick_size'])
                    if ask_a > 0:
                        bid_a = min(bid_a, round(ask_a - tick, 2))
                    if ask_b > 0:
                        bid_b = min(bid_b, round(ask_b - tick, 2))
                    bid_a = max(0.01, bid_a)
                    bid_b = max(0.01, bid_b)

                    poly_room_g = max(0, poly_max_contracts - poly_held_g)
                    p_size_g = min(poly_contracts, poly_room_g, room_g)
                    held_str_g = f" | held P:{poly_held_g}" if poly_held_g else ""
                    print(f"  {pm['team_a']} vs {pm['team_b']}{held_str_g} | "
                          f"BID {pm['team_a']}={bid_a:.2f} {pm['team_b']}={bid_b:.2f} | ${p_size_g}/side")

                    if p_size_g < 5:
                        print(f"    [POLY] room={p_size_g} < min 5 — skip")
                        continue

                    if poly_client or args.dry_run:
                        if bid_a > 0.02 and not skip_po_a:
                            if poly_client:
                                poly_client.cancel_token_orders(pm['token_a'])
                                poly_client.place_order(
                                    pm['token_a'], "BUY", bid_a, p_size_g,
                                    tick_size=pm['tick_size'], neg_risk=pm['neg_risk'],
                                    order_type="GTC", dry_run=args.dry_run)
                            elif args.dry_run:
                                print(f"    [DRY RUN] POLY BUY ${p_size_g} {pm['team_a']} @ {bid_a:.2f}")
                            total_orders += 1
                        if bid_b > 0.02 and not skip_po_b:
                            if poly_client:
                                poly_client.cancel_token_orders(pm['token_b'])
                                poly_client.place_order(
                                    pm['token_b'], "BUY", bid_b, p_size_g,
                                    tick_size=pm['tick_size'], neg_risk=pm['neg_risk'],
                                    order_type="GTC", dry_run=args.dry_run)
                            elif args.dry_run:
                                print(f"    [DRY RUN] POLY BUY ${p_size_g} {pm['team_b']} @ {bid_b:.2f}")
                            total_orders += 1
                print()

        # --- UNDER 2.5 MAPS (NO-only on KXCS2TOTALMAPS) ---
        ou_spread = args.spread
        ou_orders = 0
        for tm in totalmaps_markets:
            ticker = tm.get('ticker', '')
            title = tm.get('title', '')
            if 'over 2.5' not in title.lower():
                continue

            away_k, home_k = extract_teams(tm)
            if not away_k or not home_k:
                continue

            away_m = match_kalshi_team(away_k, team_names)
            home_m = match_kalshi_team(home_k, team_names)
            if not away_m or not home_m:
                continue

            home_count = team_match_counts.get(home_m, 0)
            away_count = team_match_counts.get(away_m, 0)
            if home_count < MIN_TEAM_MATCHES or away_count < MIN_TEAM_MATCHES:
                low_team = home_m if home_count < away_count else away_m
                low_count = min(home_count, away_count)
                print(f"    [U2.5] {away_m} vs {home_m} | SKIP ({low_team} only {low_count} matches)")
                continue

            if is_match_finished(home_m, away_m, finished_pairs) or \
               is_match_finished(home_k, away_k, finished_pairs):
                print(f"    [U2.5] {away_m} vs {home_m} | SKIP — match finished on HLTV")
                continue

            if auto_trader_live and (
                frozenset({home_m.lower(), away_m.lower()}) in auto_trader_live or
                is_match_finished(home_m, away_m, auto_trader_live) or
                is_match_finished(home_k, away_k, auto_trader_live)):
                print(f"    [U2.5] {away_m} vs {home_m} | SKIP — live on auto-trader")
                continue

            prob_home, _, _ = get_win_prob(model, encoders, home_m, away_m, scale)
            p_under = prob_under_2_5(prob_home)
            fair_under_cents = int(round(p_under * 100))
            no_target = max(1, fair_under_cents - ou_spread)

            ob = fetch_orderbook_best_ask(ticker)
            mkt_no_ask = ob['no_best_ask'] if ob and ob.get('no_best_ask') else 0
            no_bid = min(no_target, mkt_no_ask - 1) if mkt_no_ask > 0 else no_target
            no_bid = max(1, no_bid)

            if no_bid <= 2:
                continue

            game_key = '|'.join(sorted([home_m, away_m]))
            event_key_ou = '-'.join(ticker.split('-')[:-1])
            game_held = event_positions.get(event_key_ou, 0)
            max_pos = get_game_max_position(args, game_key)
            room = max(0, max_pos - game_held)
            if room == 0:
                continue

            no_contracts = min(args.contracts, args.max_contracts, room)
            no_edge = (p_under - no_bid / 100) / (no_bid / 100) if no_bid > 0 else 0
            ask_str = f" | ask={mkt_no_ask}c" if mkt_no_ask else ""
            print(f"    [U2.5] {away_m} vs {home_m} | fair={fair_under_cents}c"
                  f" | BID {no_contracts} NO @ {no_bid}c edge={no_edge:+.1%}{ask_str}")

            ok = place_limit_order(args.api_key_id, private_key, ticker, 'no',
                                   no_contracts, no_bid, args.dry_run,
                                   expiration_ts=None)
            if ok:
                ou_orders += 1
                if isinstance(ok, str):
                    this_round_orders[ok] = {'event_key': event_key_ou,
                                              'side': 'no', 'count': no_contracts,
                                              'ticker': ticker, 'price': no_bid}

        if ou_orders:
            total_orders += ou_orders
        print(f"  Under 2.5: {ou_orders} NO orders")

        prev_round_orders = this_round_orders
        print(f"  Summary: {total_orders} orders on {total_pairs} pairs")

        if args.dry_run:
            break

        print(f"  Next round at {(datetime.datetime.now() + datetime.timedelta(seconds=600)).strftime('%H:%M:%S')}")
        time.sleep(600)


# ============================================================
# SCAN / TOP / POSITIONS COMMANDS
# ============================================================

def scan_cmd(args):
    model, encoders, scale = load_model()
    print(f"  Fetching Kalshi CS2 markets...")
    try:
        markets = fetch_cs2_markets()
    except requests.exceptions.ConnectionError:
        print("  Could not connect to Kalshi API.")
        return
    except Exception as e:
        print(f"  Error: {e}")
        return
    if not markets:
        print("  No open markets found.")
        return

    edges, matched, unmatched = find_edges(model, encoders, markets, min_edge=args.min_edge, scale=scale)
    print(f"  Matched: {matched} | Unmatched: {len(unmatched)}")
    if unmatched and args.verbose:
        for u in unmatched[:10]:
            print(f"    ? {u}")
    if not edges:
        print(f"\n  No edges >= {args.min_edge:.0%}")
        return

    print(f"\n  EDGES ({len(edges)} found):")
    print(f"  {'Bet':22s} {'Matchup':35s} {'Side':3s} {'Ours':>5} {'Cost':>5} {'Edge':>6} {'Vol':>5}")
    print(f"  {'-'*22} {'-'*35} {'-'*3} {'-'*5} {'-'*5} {'-'*6} {'-'*5}")
    for e in edges:
        matchup = f"{e['away']} vs {e['home']}"
        print(f"  {e['bet_team']:22s} {matchup:35s} {e['side']:3s} "
              f"{e['our_prob']:>5.1%} {e['exec_cost']:>4.0%} "
              f"{e['edge']:>+5.1%} {e['volume']:>5d}")


def top_cmd(args):
    model, encoders, scale = load_model()
    print(f"  Fetching Kalshi CS2 markets...")
    try:
        markets = fetch_cs2_markets()
    except Exception as e:
        print(f"  Error: {e}")
        return
    if not markets:
        print("  No open markets found.")
        return

    edges, matched, unmatched = find_edges(model, encoders, markets, min_edge=0.0, no_filter=True, scale=scale)
    print(f"  Matched: {matched} | Unmatched: {len(unmatched)}")
    if not edges:
        print("\n  No edges found.")
        return

    edges.sort(key=lambda e: abs(e['edge']), reverse=True)
    top = edges[:args.top]
    print(f"\n  TOP {len(top)} EDGES (of {len(edges)} total):")
    print(f"  {'#':>3} {'Bet':22s} {'Matchup':35s} {'Side':3s} "
          f"{'Ours':>5} {'Ask':>4} {'Edge':>7} {'MinEdge':>7} {'Vol':>5} {'Pass?':>5}")
    print(f"  {'-'*3} {'-'*22} {'-'*35} {'-'*3} "
          f"{'-'*5} {'-'*4} {'-'*7} {'-'*7} {'-'*5} {'-'*5}")
    for i, e in enumerate(top, 1):
        matchup = f"{e['away']} vs {e['home']}"
        min_edge = get_dynamic_min_edge(e['our_prob'])
        passes = "YES" if e['edge'] >= min_edge else "no"
        print(f"  {i:>3} {e['bet_team']:22s} {matchup:35s} {e['side']:3s} "
              f"{e['our_prob']:>5.1%} {e['exec_cost']:>3.0%} "
              f"{e['edge']:>+6.1%} {min_edge:>6.1%} "
              f"{e['volume']:>5d} {passes:>5}")


def positions_cmd(args):
    if not args.api_key_id or not args.private_key_path:
        print("Need --api-key-id and --private-key-path")
        sys.exit(1)

    model, encoders, scale = load_model()
    private_key = load_private_key(args.private_key_path)

    print(f"  Fetching markets and positions...")
    markets = fetch_cs2_markets(pregame_only=False)

    pos_path = '/trade-api/v2/portfolio/positions'
    pos_url = KALSHI_BASE + '/portfolio/positions'
    pos_headers = make_auth_headers(private_key, args.api_key_id, 'GET', pos_path)
    resp = requests.get(pos_url, headers=pos_headers,
                        params={'settlement_status': 'unsettled'}, timeout=10)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return

    positions = [p for p in resp.json().get('market_positions', [])
                 if abs(float(p.get('position_fp', p.get('position', 0)))) > 0 and
                 any(p.get('ticker', '').startswith(s) for s in ALL_SERIES_TICKERS)]

    if not positions:
        print("  No open CS2 positions.")
        return

    team_names = list(encoders['team'].categories_[0])
    mkt_lookup = {m.get('ticker', ''): m for m in markets}

    print(f"\n  CS2 Positions ({len(positions)} markets):")
    print(f"  {'Ticker':55s} {'Pos':>5} {'AvgPx':>6} {'ModelP':>7} {'Edge':>6}")
    print(f"  {'-'*55} {'-'*5} {'-'*6} {'-'*7} {'-'*6}")

    total_risk = 0
    total_ev = 0
    for p in positions:
        ticker = p.get('ticker', '')
        pos = int(float(p.get('position_fp', p.get('position', 0))))
        avg_px = float(p.get('market_avg_price_fp', p.get('market_avg_price', 0)))
        if avg_px > 1:
            pass
        else:
            avg_px = avg_px * 100
        side = 'yes' if pos > 0 else 'no'
        qty = abs(pos)
        cost = qty * avg_px / 100

        model_prob = None
        mk = mkt_lookup.get(ticker)
        if not mk:
            try:
                mk_resp = requests.get(KALSHI_BASE + f'/markets/{ticker}', timeout=5)
                if mk_resp.status_code == 200:
                    mk = mk_resp.json().get('market', {})
            except Exception:
                pass
        if mk:
            away_k, home_k = extract_teams(mk)
            if away_k and home_k:
                home_m = match_kalshi_team(home_k, team_names)
                away_m = match_kalshi_team(away_k, team_names)
                if home_m and away_m:
                    prob_home, _, _ = get_win_prob(model, encoders, home_m, away_m, scale)
                    yes_is_home = determine_yes_team(mk, home_m, away_m)
                    if yes_is_home is not None:
                        if (yes_is_home and side == 'yes') or (not yes_is_home and side == 'no'):
                            model_prob = prob_home
                        else:
                            model_prob = 1 - prob_home

        mp_str = f"{model_prob:.1%}" if model_prob is not None else "?"
        edge = ((model_prob - avg_px / 100) / (avg_px / 100)) if model_prob is not None and avg_px > 0 else 0
        edge_str = f"{edge:+.1%}" if model_prob is not None else "?"

        total_risk += cost
        if model_prob is not None:
            total_ev += qty * model_prob - cost

        print(f"  {ticker:55s} {pos:>5d} {avg_px:>5.0f}c {mp_str:>7} {edge_str:>6}")

    print(f"\n  Total risk: ${total_risk:.2f}  |  Expected value: ${total_ev:.2f}")


def trade_cmd(args):
    """IOC trade: attempt immediate fills when edge exists."""
    apply_cap_overrides(args)
    model, encoders, scale = load_model()

    print(f"  Fetching Kalshi CS2 markets...")
    try:
        markets = fetch_cs2_markets()
    except Exception as e:
        print(f"Error: {e}")
        return
    if not markets:
        print("No markets.")
        return

    edges, matched, unmatched = find_edges(model, encoders, markets, min_edge=args.min_edge, scale=scale)
    if not edges:
        print("  No edges found.")
        return

    private_key = None
    if not args.dry_run:
        if not args.api_key_id or not args.private_key_path:
            print("Need --api-key-id and --private-key-path")
            sys.exit(1)
        private_key = load_private_key(args.private_key_path)

    forfeited_teams = fetch_hltv_forfeits()
    if forfeited_teams:
        print(f"  HLTV: {len(forfeited_teams)} forfeited team(s): {', '.join(forfeited_teams)}")

    ff_model_data, ff_power_map = load_forfeit_model()
    ff_all_df = None
    if ff_model_data:
        from train_model import load_matches as _load_matches_ff
        ff_all_df = _load_matches_ff(include_forfeits=True)
        print(f"  Forfeit model: loaded")

    ledger = load_trade_ledger()

    game_positions = {}
    if private_key and not args.dry_run:
        gp, _ = get_live_positions(args.api_key_id, private_key, markets)
        if gp:
            game_positions = gp

    print(f"\n  [TRADE - {'DRY RUN' if args.dry_run else 'LIVE'}] "
          f"{len(edges)} edges | contracts={args.contracts} | IOC")

    for e in edges:
        ticker = e['ticker']
        side = e['side']
        ask_price = e['exec_price_cents']
        prob = e['our_prob']
        edge = e['edge']
        bet_team = e['bet_team']
        game_key = '|'.join(sorted([e['home'], e['away']]))
        matchup = f"{e['away']} vs {e['home']}"

        if forfeited_teams:
            if e['home'].lower() in forfeited_teams or e['away'].lower() in forfeited_teams:
                ff_team = e['home'] if e['home'].lower() in forfeited_teams else e['away']
                print(f"  SKIP {matchup}: {ff_team} forfeited on HLTV")
                continue

        if ff_model_data and ff_all_df is not None:
            from train_model import predict_forfeit
            ff_prob = predict_forfeit(ff_model_data, bet_team,
                                      e['away'] if bet_team == e['home'] else e['home'],
                                      ff_power_map, ff_all_df)
            if ff_prob >= FF_RATE_THRESHOLD:
                print(f"  SKIP {matchup}: betting on {bet_team} but P(forfeit)={ff_prob:.1%}")
                continue

        mult = get_edge_multiplier(ledger, game_key)
        trade_min = get_dynamic_min_edge(prob) * mult
        if edge < trade_min:
            print(f"  SKIP {matchup}: edge {edge:.1%} < min {trade_min:.1%}")
            continue

        game_held = game_positions.get(game_key, 0)
        max_pos = get_game_max_position(args, game_key)
        room = max(0, max_pos - game_held)
        if room == 0:
            print(f"  SKIP {matchup}: capped at {max_pos}")
            continue

        contracts = min(args.contracts, room)
        print(f"  {matchup} | {ticker}")
        print(f"    BUY {contracts} {side.upper()} ({bet_team}) @ {ask_price}c | "
              f"prob={prob:.1%} edge={edge:+.1%}")

        result = place_ioc_order(args.api_key_id, private_key, ticker, side,
                                 contracts, ask_price, args.dry_run)
        if result and result > 0:
            ledger[game_key] = ledger.get(game_key, 0) + 1
            save_trade_ledger(ledger)



# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="CS2 Kalshi edge scanner + house mode")
    sub = ap.add_subparsers(dest='cmd')

    sc = sub.add_parser('scan')
    sc.add_argument('--min-edge', type=float, default=0.10)
    sc.add_argument('-v', '--verbose', action='store_true')

    tp = sub.add_parser('top', help='Show top N edges sorted by size')
    tp.add_argument('--top', type=int, default=25)

    ps = sub.add_parser('positions', help='Show current positions with model prob')
    ps.add_argument('--api-key-id', type=str, default=None)
    ps.add_argument('--private-key-path', type=str, default=None)

    tr = sub.add_parser('trade')
    tr.add_argument('--min-edge', type=float, default=0.10)
    tr.add_argument('--contracts', type=int, default=25)
    tr.add_argument('--api-key-id', type=str, default=None)
    tr.add_argument('--private-key-path', type=str, default=None)
    tr.add_argument('--dry-run', action='store_true')
    tr.add_argument('--max-position', type=int, default=MAX_PER_MARKET_TYPE)

    mm = sub.add_parser('limit', help='House mode — resting bids on both sides')
    mm.add_argument('--contracts', type=int, default=150)
    mm.add_argument('--max-contracts', type=int, default=300)
    mm.add_argument('--max-position', type=int, default=MAX_PER_MARKET_TYPE)
    mm.add_argument('--spread', type=int, default=8)
    mm.add_argument('--no-book', action='store_true',
                    help='Skip orderbook — post at fair minus spread')
    mm.add_argument('--api-key-id', type=str, default=None)
    mm.add_argument('--private-key-path', type=str, default=None)
    mm.add_argument('--dry-run', action='store_true')
    mm.add_argument('--poly-key-path', type=str, default=None,
                    help='Polymarket private key file (enables dual-exchange)')
    mm.add_argument('--poly-contracts', type=int, default=None,
                    help='Polymarket $ per side (default: same as --contracts)')
    mm.add_argument('--best-of', type=int, default=3, choices=[1, 3, 5],
                    help='Match format (default: 3, use 1 for Major stages)')

    cl = sub.add_parser('cap-log', help='Review cap hit log / set overrides')
    cl.add_argument('--clear', action='store_true')
    cl.add_argument('--max-position', type=int, default=None)
    cl.add_argument('--game', type=str, nargs=2, action='append', default=None,
                    metavar=('MATCHUP', 'CAP'))
    cl.add_argument('--reset', action='store_true')

    args = ap.parse_args()

    if args.cmd == 'cap-log':
        if args.clear:
            if os.path.exists(CAP_LOG_PATH):
                os.remove(CAP_LOG_PATH)
                print("  Cap hit log cleared.")
        if args.reset:
            removed = False
            for f in [CAP_OVERRIDE_PATH, NOTIFIED_CAPS_PATH]:
                if os.path.exists(f):
                    os.remove(f)
                    removed = True
            print("  Cap overrides cleared." if removed else "  No overrides to clear.")
        has_override = False
        if args.max_position is not None or args.game:
            overrides = load_cap_overrides()
            if args.max_position is not None:
                overrides['max_position'] = args.max_position
            if args.game:
                games = overrides.get('games', {})
                for matchup, cap_str in args.game:
                    parts = re.split(r'\s+(?:vs\.?|@|at|\|)\s+', matchup.strip(), maxsplit=1)
                    if len(parts) == 2:
                        game_key = '|'.join(sorted([p.strip() for p in parts]))
                    else:
                        game_key = matchup.strip()
                    games[game_key] = int(cap_str)
                overrides['games'] = games
            save_cap_overrides(overrides)
            print(f"  Cap overrides saved.")
            has_override = True
        if not args.clear and not args.reset and not has_override:
            print_cap_log()
    elif args.cmd == 'scan':
        scan_cmd(args)
    elif args.cmd == 'top':
        top_cmd(args)
    elif args.cmd == 'positions':
        positions_cmd(args)
    elif args.cmd == 'trade':
        trade_cmd(args)
    elif args.cmd == 'limit':
        limit_cmd(args)
    else:
        args.min_edge = 0.10
        args.verbose = False
        scan_cmd(args)


if __name__ == '__main__':
    main()