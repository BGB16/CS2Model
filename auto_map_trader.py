#!/usr/bin/env python3
"""
auto_map_trader.py — Automated map-change liquidity poster for CS2 Kalshi markets.

Monitors HLTV for live BO3 map completions, matches them to Kalshi markets,
recomputes series fair prices from the updated map score, and posts maker-only
limit orders on both sides. Orders auto-cancel after 2 minutes. Only posts
once per map change — no reposting.

Defaults: 100 contracts/side, 4c spread from fair, 2-min TTL.

Usage:
    python3 auto_map_trader.py --api-key-id KEY --private-key-path key.pem
    python3 auto_map_trader.py --dry-run
"""

import argparse
import json
import os
import sys
import time
import threading
import requests
from datetime import datetime

from kalshi_edge import (
    load_private_key,
    SERIES_TICKER,
    fetch_cs2_markets, extract_teams, detect_market_type,
    determine_yes_team, match_kalshi_team,
    load_model, get_win_prob,
    fetch_orderbook_best_ask, place_limit_order,
    cancel_orders_for_ticker,
)

from live_trader import (
    series_to_map_prob, live_bo3_win_prob,
    series_to_map_prob_bo5, live_bo5_win_prob,
)

from hltv_map_monitor import get_live_match_urls, get_map_scores, count_maps_won, get_hltv_browser, close_hltv_browser

GAMMA_BASE = "https://gamma-api.polymarket.com"


def fetch_poly_live_matches(model_team_names):
    """Fetch live CS2 matches from Polymarket Gamma API.
    Returns dict keyed by event slug, same shape as HLTV's get_live_match_urls."""
    try:
        resp = requests.get(f"{GAMMA_BASE}/events",
                            params={'series_slug': 'counter-strike',
                                    'closed': 'false', 'limit': 100},
                            timeout=15)
        if resp.status_code != 200:
            print(f"  [Poly] Gamma API error: {resp.status_code}")
            return {}
    except Exception as e:
        print(f"  [Poly] Gamma API error: {e}")
        return {}

    matches = {}
    for event in resp.json():
        if not event.get('live'):
            continue
        score_str = event.get('score', '')
        if not score_str:
            continue

        parts = score_str.split('|')
        if len(parts) < 3:
            continue
        map_score = parts[1]
        best_of = parts[2]

        map_parts = map_score.split('-')
        if len(map_parts) != 2:
            continue
        try:
            t1_maps = int(map_parts[0])
            t2_maps = int(map_parts[1])
        except ValueError:
            continue

        title = event.get('title', '')
        slug = event.get('slug', '')

        team1, team2 = None, None
        for mkt in event.get('markets', []):
            if mkt.get('sportsMarketType') == 'moneyline':
                outcomes = json.loads(mkt.get('outcomes', '[]'))
                if len(outcomes) >= 2:
                    team1, team2 = outcomes[0], outcomes[1]
                break
        if not team1 or not team2:
            outcomes_re = __import__('re').findall(r':\s*(.+?)\s+vs\s+(.+?)\s*\(', title)
            if outcomes_re:
                team1, team2 = outcomes_re[0]
            else:
                continue

        matches[slug] = {
            'url': f"https://polymarket.com/event/{slug}",
            'team1': team1,
            'team2': team2,
            'best_of': best_of,
            'maps_won': (t1_maps, t2_maps),
            'score_raw': score_str,
            'slug': slug,
        }
    return matches


def fetch_poly_map_scores(slug):
    """Re-fetch a single event's map score from Polymarket."""
    try:
        resp = requests.get(f"{GAMMA_BASE}/events",
                            params={'slug': slug}, timeout=10)
        if resp.status_code != 200:
            return None
        events = resp.json()
        if not events:
            return None
        score_str = events[0].get('score', '')
        parts = score_str.split('|')
        if len(parts) < 3:
            return None
        map_parts = parts[1].split('-')
        if len(map_parts) != 2:
            return None
        return (int(map_parts[0]), int(map_parts[1]))
    except Exception:
        return None

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
LIVE_MATCHES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'live_matches.json')

CONTRACTS = 100
SPREAD_CENTS = 4
ORDER_TTL = 120
POLL_INTERVAL = 60


def alert(message):
    print(f"\n*** ALERT [{datetime.now().strftime('%H:%M:%S')}] {message}")
    try:
        import subprocess
        subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"])
    except Exception:
        print("\a")


def fetch_kalshi_matches():
    try:
        markets = fetch_cs2_markets(pregame_only=False,
                                    series_filter=[SERIES_TICKER])
    except Exception as e:
        print(f"  [Kalshi] Error fetching markets: {e}")
        return {}

    model_path = os.path.join(DATA_DIR, 'model', 'model.pkl')
    if not os.path.exists(model_path):
        print("  [Kalshi] No model found")
        return {}

    model, encoders, scale = load_model()
    team_names = list(encoders['team'].categories_[0])

    result = {}
    for m in markets:
        away_k, home_k = extract_teams(m)
        if not away_k or not home_k:
            continue

        away_matched = match_kalshi_team(away_k, team_names)
        home_matched = match_kalshi_team(home_k, team_names)
        if not away_matched or not home_matched:
            continue

        ticker = m.get('ticker', '')
        mtype = detect_market_type(m)
        if mtype != 'moneyline':
            continue

        key = (home_matched, away_matched)
        if key not in result:
            prob = 0.5
            try:
                prob, _, _ = get_win_prob(model, encoders, home_matched, away_matched, scale)
            except Exception:
                pass
            result[key] = {
                'home': home_matched, 'away': away_matched,
                'home_prob': round(float(prob), 4),
                'tickers': [],
            }

        yes_is_home = determine_yes_team(m, home_matched, away_matched)
        if yes_is_home is None:
            yes_is_home = True
        result[key]['tickers'].append({
            'ticker': ticker,
            'home_is_yes': yes_is_home,
            'yes_team': home_matched if yes_is_home else away_matched,
            'no_team': away_matched if yes_is_home else home_matched,
        })

    return result


def match_hltv_to_kalshi(hltv_team1, hltv_team2, kalshi_matches, model_team_names):
    t1_matched = match_kalshi_team(hltv_team1, model_team_names)
    t2_matched = match_kalshi_team(hltv_team2, model_team_names)
    if not t1_matched or not t2_matched:
        return None, None

    key1 = (t1_matched, t2_matched)
    key2 = (t2_matched, t1_matched)
    if key1 in kalshi_matches:
        return key1, True
    if key2 in kalshi_matches:
        return key2, False
    return None, None


def post_and_cancel(api_key_id, private_key, match_info, home_fair, contracts,
                    spread, ttl, dry_run,
                    poly_client=None, poly_market=None, poly_contracts=None,
                    home_team_name=None):
    tickers = match_info['tickers'] if match_info else []
    home = match_info['home'] if match_info else (home_team_name or '')
    away = match_info['away'] if match_info else ''
    away_fair = 100 - home_fair
    home_bid = max(1, home_fair - spread)
    away_bid = max(1, away_fair - spread)

    if home_bid > home_fair:
        home_bid = home_fair
    if away_bid > away_fair:
        away_bid = away_fair

    posted_tickers = set()
    poly_posted_tokens = []

    # --- Kalshi orders ---
    for t in tickers:
        ticker = t['ticker']
        home_is_yes = t['home_is_yes']
        yes_bid = home_bid if home_is_yes else away_bid
        no_bid = away_bid if home_is_yes else home_bid
        yes_team = home if home_is_yes else away
        no_team = away if home_is_yes else home
        yes_fair = home_fair if home_is_yes else away_fair
        no_fair = away_fair if home_is_yes else home_fair

        ob = fetch_orderbook_best_ask(ticker)
        if ob:
            if ob.get('yes_best_ask') is not None and yes_bid >= ob['yes_best_ask']:
                capped = ob['yes_best_ask'] - 1
                print(f"    [MAKER] {yes_team} YES: {yes_bid}c -> {capped}c "
                      f"(ask={ob['yes_best_ask']}c)")
                yes_bid = capped
            if ob.get('no_best_ask') is not None and no_bid >= ob['no_best_ask']:
                capped = ob['no_best_ask'] - 1
                print(f"    [MAKER] {no_team} NO: {no_bid}c -> {capped}c "
                      f"(ask={ob['no_best_ask']}c)")
                no_bid = capped

        yes_bid = max(1, yes_bid)
        no_bid = max(1, no_bid)

        if 1 <= yes_bid <= 99:
            print(f"    [K] BID {contracts} {yes_team} YES @ {yes_bid}c (fair={yes_fair}c)")
            place_limit_order(api_key_id, private_key, ticker, 'yes',
                              contracts, yes_bid, dry_run)
            posted_tickers.add(ticker)

        if 1 <= no_bid <= 99:
            print(f"    [K] BID {contracts} {no_team} NO @ {no_bid}c (fair={no_fair}c)")
            place_limit_order(api_key_id, private_key, ticker, 'no',
                              contracts, no_bid, dry_run)
            posted_tickers.add(ticker)

    # --- Polymarket orders ---
    if poly_market and (poly_client or dry_run):
        from poly_cs2 import get_poly_orderbook, get_best_ask as poly_best_ask
        pm = poly_market
        p_size = poly_contracts or contracts
        spread_dec = spread / 100

        pm_team_a_matched = pm.get('home_team', pm['team_a'])
        team_a_is_home = (pm_team_a_matched == home) if home else (pm_team_a_matched == home_team_name)
        if team_a_is_home:
            bid_a = round(max(0.01, home_fair / 100 - spread_dec), 2)
            bid_b = round(max(0.01, away_fair / 100 - spread_dec), 2)
        else:
            bid_a = round(max(0.01, away_fair / 100 - spread_dec), 2)
            bid_b = round(max(0.01, home_fair / 100 - spread_dec), 2)

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

        print(f"    [P] BID ${p_size} {pm['team_a']} @ {bid_a:.2f} (ask={ask_a:.2f}) | "
              f"${p_size} {pm['team_b']} @ {bid_b:.2f} (ask={ask_b:.2f})")

        if bid_a > 0.02:
            if poly_client:
                poly_client.cancel_token_orders(pm['token_a'])
                poly_client.place_order(pm['token_a'], "BUY", bid_a, p_size,
                                        tick_size=pm['tick_size'], neg_risk=pm['neg_risk'],
                                        order_type="GTC", dry_run=dry_run)
            elif dry_run:
                print(f"      [DRY RUN] POLY BUY ${p_size} {pm['team_a']} @ {bid_a:.2f}")
            poly_posted_tokens.append(pm['token_a'])
        if bid_b > 0.02:
            if poly_client:
                poly_client.cancel_token_orders(pm['token_b'])
                poly_client.place_order(pm['token_b'], "BUY", bid_b, p_size,
                                        tick_size=pm['tick_size'], neg_risk=pm['neg_risk'],
                                        order_type="GTC", dry_run=dry_run)
            elif dry_run:
                print(f"      [DRY RUN] POLY BUY ${p_size} {pm['team_b']} @ {bid_b:.2f}")
            poly_posted_tokens.append(pm['token_b'])

    if not posted_tickers and not poly_posted_tokens:
        print(f"    No orders placed (bids out of range)")
        return

    cancel_time = datetime.now()
    cancel_time_str = f"{cancel_time.hour}:{cancel_time.minute + ttl // 60:02d}:{cancel_time.second:02d}"
    print(f"    Orders live — will cancel in {ttl}s (~{cancel_time_str})")

    def _cancel():
        time.sleep(ttl)
        if dry_run:
            print(f"    [DRY RUN] TTL expired — would cancel orders")
            return
        total = 0
        for ticker in posted_tickers:
            n = cancel_orders_for_ticker(api_key_id, private_key, ticker)
            total += n
        if poly_client and poly_posted_tokens:
            for tok in poly_posted_tokens:
                poly_client.cancel_token_orders(tok)
                total += 1
        print(f"    [TTL] Cancelled {total} order(s) after {ttl}s")

    threading.Thread(target=_cancel, daemon=True).start()


def write_live_matches(match_meta, kalshi_matches):
    entries = []
    for mid, mi in match_meta.items():
        entry = {'team1': mi.get('team1', ''), 'team2': mi.get('team2', '')}
        kkey = mi.get('kalshi_key')
        if kkey and kkey in kalshi_matches:
            km = kalshi_matches[kkey]
            entry['home'] = km['home']
            entry['away'] = km['away']
        entries.append(entry)
    tmp = LIVE_MATCHES_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({'live': entries, 'updated': datetime.now().isoformat()}, f)
    os.replace(tmp, LIVE_MATCHES_FILE)


def main():
    parser = argparse.ArgumentParser(description="Auto map-change liquidity poster")
    parser.add_argument('--api-key-id', type=str, default=None)
    parser.add_argument('--private-key-path', type=str, default=None)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--contracts', type=int, default=CONTRACTS,
                        help='Contracts per side (default: 100)')
    parser.add_argument('--spread', type=int, default=SPREAD_CENTS,
                        help='Cents from fair per side (default: 4)')
    parser.add_argument('--ttl', type=int, default=ORDER_TTL,
                        help='Order TTL in seconds (default: 120)')
    parser.add_argument('--interval', type=int, default=POLL_INTERVAL,
                        help='Poll interval in seconds (default: 60)')
    parser.add_argument('--score-source', choices=['hltv', 'poly'], default='hltv',
                        help='Live score source: hltv (default) or poly (Polymarket)')
    parser.add_argument('--poly-key-path', type=str, default=None,
                        help='Polymarket private key file (enables dual-exchange posting)')
    parser.add_argument('--poly-contracts', type=int, default=None,
                        help='Polymarket $ per side (default: same as --contracts)')
    args = parser.parse_args()

    dry_run = args.dry_run
    api_key_id = args.api_key_id
    private_key = None
    use_poly_scores = args.score_source == 'poly'
    poly_contracts = args.poly_contracts or args.contracts

    if not dry_run:
        if not api_key_id or not args.private_key_path:
            print("Error: need --api-key-id and --private-key-path (or use --dry-run)")
            sys.exit(1)
        private_key = load_private_key(args.private_key_path)

    # Polymarket setup
    poly_client = None
    poly_market_lookup = {}
    if args.poly_key_path:
        from poly_cs2 import (PolyClient, fetch_poly_cs2_markets, build_poly_market_lookup,
                               get_poly_orderbook, get_best_ask as poly_best_ask)
        if not dry_run:
            poly_client = PolyClient(args.poly_key_path)
            poly_client.start_heartbeat()

    model, encoders, _ = load_model()
    model_team_names = list(encoders['team'].categories_[0])

    mode = "DRY RUN" if dry_run else "LIVE"
    score_src = "Polymarket" if use_poly_scores else "HLTV"
    print(f"\n  Auto Map Trader — {mode} (scores: {score_src})")
    print(f"  Contracts: {args.contracts}/side | Spread: {args.spread}c | "
          f"TTL: {args.ttl}s | Poll: {args.interval}s")
    print(f"  Press Ctrl+C to stop.\n")

    session = None
    if not use_poly_scores:
        print("  Starting HLTV browser...", end=" ", flush=True)
        get_hltv_browser()
        print("done.\n")

    prev_maps_won = {}
    match_meta = {}
    posted_states = set()

    print("  Fetching Kalshi CS2 markets...")
    kalshi_matches = fetch_kalshi_matches()
    if kalshi_matches:
        print(f"  {len(kalshi_matches)} Kalshi match(es):")
        for key, info in kalshi_matches.items():
            n_tickers = len(info['tickers'])
            print(f"    {info['away']} vs {info['home']} — "
                  f"{info['home_prob']:.0%} ({n_tickers} ticker(s))")
    else:
        print("  No Kalshi CS2 markets found — will retry.")

    if args.poly_key_path:
        _poly_mkts = fetch_poly_cs2_markets(pregame_only=False, our_teams=model_team_names)
        poly_market_lookup = build_poly_market_lookup(_poly_mkts)
        print(f"  {len(poly_market_lookup)} Poly CS2 match(es)")
    print()

    last_kalshi_refresh = time.time()

    while True:
        try:
            if time.time() - last_kalshi_refresh > 300:
                kalshi_matches = fetch_kalshi_matches()
                last_kalshi_refresh = time.time()
                if kalshi_matches:
                    print(f"  [Refresh] {len(kalshi_matches)} Kalshi market(s)")
                if args.poly_key_path:
                    _poly_mkts = fetch_poly_cs2_markets(pregame_only=False, our_teams=model_team_names)
                    poly_market_lookup = build_poly_market_lookup(_poly_mkts)
                    print(f"  [Refresh] {len(poly_market_lookup)} Poly market(s)")

            if use_poly_scores:
                live = fetch_poly_live_matches(model_team_names)
            else:
                live = get_live_match_urls(session)

            if not live:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] No live matches.", flush=True)
                time.sleep(args.interval)
                continue

            for mid in list(prev_maps_won):
                if mid not in live:
                    old = prev_maps_won.pop(mid)
                    info = match_meta.pop(mid, {})
                    posted_states = {s for s in posted_states if s[0] != mid}
                    final = old
                    if use_poly_scores:
                        refreshed = fetch_poly_map_scores(mid)
                        if refreshed:
                            final = refreshed
                    else:
                        url = info.get('url', '')
                        if url:
                            try:
                                ms = get_map_scores(session, url)
                                final = count_maps_won(ms)
                            except Exception:
                                pass
                    print(f"\n  Match ended: {info.get('team1','?')} vs "
                          f"{info.get('team2','?')} — final: {final[0]}-{final[1]}")

            for mid, info in live.items():
                if use_poly_scores:
                    w = info.get('maps_won')
                    if w is None:
                        continue
                else:
                    time.sleep(1)
                    try:
                        map_scores = get_map_scores(session, info["url"])
                    except Exception as e:
                        continue
                    w = count_maps_won(map_scores)

                if mid not in prev_maps_won:
                    prev_maps_won[mid] = w
                    mi = dict(info)
                    mi['kalshi_key'] = None
                    mi['t1_is_home'] = None

                    if kalshi_matches:
                        kkey, t1_is_home = match_hltv_to_kalshi(
                            info['team1'], info['team2'],
                            kalshi_matches, model_team_names)
                        mi['kalshi_key'] = kkey
                        mi['t1_is_home'] = t1_is_home

                    match_meta[mid] = mi

                    exch_tags = []
                    if mi['kalshi_key']:
                        km_info = kalshi_matches[mi['kalshi_key']]
                        exch_tags.append(f"K:{km_info['home_prob']:.0%}")
                    if poly_market_lookup:
                        t1m = match_kalshi_team(info['team1'], model_team_names)
                        t2m = match_kalshi_team(info['team2'], model_team_names)
                        if t1m and t2m:
                            gk = '|'.join(sorted([t1m, t2m]))
                            if gk in poly_market_lookup:
                                exch_tags.append("P")
                    tag = '+'.join(exch_tags) if exch_tags else "NO MATCH"
                    print(f"  Tracking: {info['team1']} vs {info['team2']} "
                          f"maps: {w[0]}-{w[1]} ({info['best_of']}) [{tag}]")

                    # Cancel house-mode orders immediately so they don't linger
                    cancelled_any = False
                    if mi['kalshi_key'] and not dry_run and private_key:
                        km_info = kalshi_matches[mi['kalshi_key']]
                        for t in km_info['tickers']:
                            n = cancel_orders_for_ticker(api_key_id, private_key, t['ticker'])
                            if n:
                                cancelled_any = True
                    if poly_client and poly_market_lookup:
                        t1m = match_kalshi_team(info['team1'], model_team_names)
                        t2m = match_kalshi_team(info['team2'], model_team_names)
                        if t1m and t2m:
                            gk = '|'.join(sorted([t1m, t2m]))
                            _pm = poly_market_lookup.get(gk)
                            if _pm:
                                poly_client.cancel_token_orders(_pm['token_a'])
                                poly_client.cancel_token_orders(_pm['token_b'])
                                cancelled_any = True
                    if cancelled_any:
                        print(f"    Cancelled house-mode orders for {info['team1']} vs {info['team2']}")
                    write_live_matches(match_meta, kalshi_matches)
                    continue

                old_w = prev_maps_won[mid]
                if old_w == w:
                    continue

                prev_maps_won[mid] = w
                mi = match_meta.get(mid, info)

                alert(f"Map finished! {info['team1']} vs {info['team2']}: "
                      f"maps {old_w[0]}-{old_w[1]} -> {w[0]}-{w[1]}")

                state_key = (mid, w[0], w[1])
                if state_key in posted_states:
                    print(f"    Already posted for {w[0]}-{w[1]} — skipping")
                    continue

                kkey = mi.get('kalshi_key')
                t1_is_home = mi.get('t1_is_home')

                if not kkey or kkey not in kalshi_matches:
                    if kalshi_matches:
                        kkey, t1_is_home = match_hltv_to_kalshi(
                            info['team1'], info['team2'],
                            kalshi_matches, model_team_names)
                        mi['kalshi_key'] = kkey
                        mi['t1_is_home'] = t1_is_home
                        match_meta[mid] = mi

                km = kalshi_matches.get(kkey) if kkey else None

                # Look up matching Poly market by game_key
                pm = None
                if poly_market_lookup:
                    t1m = match_kalshi_team(info['team1'], model_team_names)
                    t2m = match_kalshi_team(info['team2'], model_team_names)
                    if t1m and t2m:
                        gk = '|'.join(sorted([t1m, t2m]))
                        pm = poly_market_lookup.get(gk)

                if not km and not pm:
                    print(f"    No Kalshi or Poly match — skipping trade")
                    posted_states.add(state_key)
                    continue

                best_of = info.get('best_of', '')

                is_bo5 = '5' in best_of
                is_bo3 = '3' in best_of

                if not is_bo3 and not is_bo5:
                    print(f"    Not BO3/BO5 ({best_of}) — skipping")
                    posted_states.add(state_key)
                    continue

                # Determine home/away orientation
                if km:
                    if t1_is_home:
                        home_maps, away_maps = w
                    else:
                        away_maps, home_maps = w
                    home_name = km['home']
                    away_name = km['away']
                    home_prob = km['home_prob']
                else:
                    # Poly-only: use matched team names, t1 = home
                    t1m = match_kalshi_team(info['team1'], model_team_names)
                    t2m = match_kalshi_team(info['team2'], model_team_names)
                    home_maps, away_maps = w
                    home_name = t1m or info['team1']
                    away_name = t2m or info['team2']
                    try:
                        home_prob, _, _ = get_win_prob(model, encoders, home_name, away_name)
                    except Exception:
                        home_prob = 0.5

                wins_needed = 3 if is_bo5 else 2
                if home_maps >= wins_needed or away_maps >= wins_needed:
                    print(f"    Series decided {home_maps}-{away_maps} — skipping")
                    posted_states.add(state_key)
                    continue

                maps_played = home_maps + away_maps

                if is_bo5:
                    map_p = series_to_map_prob_bo5(home_prob)
                    per_map = [map_p] * 5
                    live_wp = live_bo5_win_prob(home_maps, away_maps, maps_played, per_map)
                else:
                    map_p = series_to_map_prob(home_prob)
                    per_map = [map_p] * 3
                    live_wp = live_bo3_win_prob(home_maps, away_maps, maps_played, per_map)

                home_fair = max(1, min(99, int(round(live_wp * 100))))

                fmt = "BO5" if is_bo5 else "BO3"
                exchanges = []
                if km:
                    exchanges.append("K")
                if pm:
                    exchanges.append("P")
                print(f"    {home_name} {home_maps}-{away_maps} {away_name} "
                      f"({fmt}) [{'+'.join(exchanges)}]")
                print(f"    Pregame: {home_prob:.0%} -> Live: {live_wp:.0%} "
                      f"-> Fair: {home_fair}c / {100-home_fair}c")

                post_and_cancel(
                    api_key_id, private_key, km, home_fair,
                    args.contracts, args.spread, args.ttl, dry_run,
                    poly_client=poly_client, poly_market=pm,
                    poly_contracts=poly_contracts,
                    home_team_name=home_name)

                posted_states.add(state_key)

            write_live_matches(match_meta, kalshi_matches)

            ts = datetime.now().strftime("%H:%M:%S")
            summary = ", ".join(
                f"{match_meta[mid]['team1']} {w[0]}-{w[1]} {match_meta[mid]['team2']}"
                for mid, w in prev_maps_won.items() if mid in match_meta
            )
            print(f"  [{ts}] {summary or 'no live matches'}", flush=True)

        except Exception as e:
            print(f"\n  Error: {e}")
            import traceback
            traceback.print_exc()

        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        close_hltv_browser()
        try:
            os.remove(LIVE_MATCHES_FILE)
        except FileNotFoundError:
            pass