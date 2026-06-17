"""
roster_monitor.py — Alert when a team with a live Kalshi CS2 market has a recent roster change.

Scrapes Liquipedia CS2 transfers portal and cross-references against open Kalshi markets.

Usage:
  python3 roster_monitor.py                # one-shot check (last 3 days)
  python3 roster_monitor.py --days 5       # look back 5 days
  python3 roster_monitor.py --loop 600     # re-check every 10 min
"""
import re, sys, time, argparse, datetime
import requests

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKERS = ["KXCS2GAME", "KXCS2MAP", "KXCS2TOTALMAPS"]
LIQUIPEDIA_URL = "https://liquipedia.net/counterstrike/Portal:Transfers"


def normalize(name):
    if not name:
        return ''
    return re.sub(r'\s+', ' ', name.replace('_', ' ').strip()).lower()


def fetch_kalshi_teams():
    """Fetch all team names from open Kalshi CS2 markets.
    Returns dict {normalized_name: display_name} and set of matchup strings."""
    teams = {}
    matchups = {}
    now = datetime.datetime.now(datetime.timezone.utc)

    for series in SERIES_TICKERS:
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
                break
            data = resp.json()
            batch = data.get('markets', [])
            for m in batch:
                if m.get('status', '').lower() in ('closed', 'settled', 'finalized'):
                    continue
                exp_str = m.get('expected_expiration_time', '')
                if exp_str:
                    try:
                        exp_dt = datetime.datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
                        if (exp_dt - now).total_seconds() <= 0:
                            continue
                    except (ValueError, TypeError):
                        pass
                title = m.get('title', '') or ''
                match = re.search(r'the\s+(.+?)\s+vs\.?\s+(.+?)\s+(?:CS2\s+)?match', title, re.IGNORECASE)
                if match:
                    t1, t2 = match.group(1).strip(), match.group(2).strip()
                    teams[normalize(t1)] = t1
                    teams[normalize(t2)] = t2
                    key = ' vs '.join(sorted([t1, t2]))
                    matchups[key] = (t1, t2)
            cursor = data.get('cursor', '')
            if not cursor or not batch:
                break

    return teams, matchups


def match_liquipedia_to_kalshi(liqui_name, kalshi_teams):
    """Try to match a Liquipedia team name to a Kalshi team.
    Returns (kalshi_normalized, kalshi_display) or None."""
    ln = normalize(liqui_name)
    if not ln:
        return None

    if ln in kalshi_teams:
        return (ln, kalshi_teams[ln])

    for kn, display in kalshi_teams.items():
        if len(kn) >= 4 and kn in ln:
            return (kn, display)
        if len(ln) >= 4 and ln in kn:
            return (kn, display)

    ln_tokens = set(ln.split())
    for kn, display in kalshi_teams.items():
        kt = set(kn.split())
        overlap = len(ln_tokens & kt)
        if kt and overlap >= max(1, len(kt) // 2 + 1):
            return (kn, display)

    return None


def fetch_roster_changes(lookback_days):
    """Scrape Liquipedia transfers for recent player departures/benchings.
    Returns dict {team_name: {'lost': int, 'inactive': int, 'players': [str], 'dates': [str]}}."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("  ERROR: bs4 not installed (pip install beautifulsoup4)")
        return {}

    try:
        resp = requests.get(LIQUIPEDIA_URL,
                            headers={'User-Agent': 'CS2Model/1.0 (blakegordon2003@gmail.com)',
                                     'Accept-Encoding': 'gzip'},
                            timeout=15)
        if resp.status_code != 200:
            print(f"  Liquipedia HTTP {resp.status_code}")
            return {}
    except Exception as e:
        print(f"  Liquipedia fetch failed: {e}")
        return {}

    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('div', class_='divTable')
    if not table:
        print("  No transfer table found")
        return {}

    cutoff = (datetime.datetime.now() - datetime.timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    changes = {}

    for row in table.find_all('div', class_='divRow'):
        date_cell = row.find('div', class_='Date')
        if not date_cell:
            continue
        date_str = date_cell.get_text(strip=True)
        if date_str < cutoff:
            break

        row_classes = row.get('class', [])

        old_cell = row.find('div', class_='OldTeam')
        new_cell = row.find('div', class_='NewTeam')
        if not old_cell or not new_cell:
            continue

        old_text = old_cell.get_text(' ', strip=True)
        new_text = new_cell.get_text(' ', strip=True)

        if '(Coach)' in old_text or '(Coach)' in new_text:
            continue

        old_ts = old_cell.find('span', attrs={'data-highlighting-class': True})
        old_team = old_ts['data-highlighting-class'] if old_ts else ''

        affected_team = ''
        change_type = ''

        if 'mainpage-transfer-from-team' in row_classes and old_team:
            affected_team = old_team
            change_type = 'lost'
        elif 'mainpage-transfer-neutral' in row_classes and old_team:
            if '(Inactive)' in new_text or '(Benched)' in new_text:
                affected_team = old_team
                change_type = 'inactive'

        if not affected_team:
            continue

        name_cell = row.find('div', class_='Name')
        players = []
        if name_cell:
            for bp in name_cell.find_all('div', class_='block-player'):
                ns = bp.find('span', class_='name')
                if ns:
                    players.append(ns.get_text(strip=True))

        if not players:
            continue

        if affected_team not in changes:
            changes[affected_team] = {'lost': 0, 'inactive': 0, 'players': [], 'dates': []}

        if change_type == 'lost':
            changes[affected_team]['lost'] += len(players)
        else:
            changes[affected_team]['inactive'] += len(players)
        changes[affected_team]['players'].extend(players)
        if date_str not in changes[affected_team]['dates']:
            changes[affected_team]['dates'].append(date_str)

    return changes


def run_check(lookback_days):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f"\n{'='*60}")
    print(f"  ROSTER MONITOR — {now}")
    print(f"  Lookback: {lookback_days} days")
    print(f"{'='*60}")

    print(f"\n  Fetching Kalshi CS2 markets...")
    kalshi_teams, matchups = fetch_kalshi_teams()
    if not kalshi_teams:
        print("  No open Kalshi CS2 markets found.")
        return
    print(f"  {len(kalshi_teams)} teams across {len(matchups)} matchups\n")

    print(f"  Fetching Liquipedia transfers...")
    roster_changes = fetch_roster_changes(lookback_days)
    if not roster_changes:
        print("  No recent roster changes found.\n")
        return
    print(f"  {len(roster_changes)} teams with roster changes\n")

    alerts = []
    for liqui_team, info in roster_changes.items():
        result = match_liquipedia_to_kalshi(liqui_team, kalshi_teams)
        if result:
            kn, display = result
            affected_matchups = [f"{t1} vs {t2}" for key, (t1, t2) in matchups.items()
                                 if normalize(t1) == kn or normalize(t2) == kn]
            alerts.append({
                'kalshi_team': display,
                'liqui_team': liqui_team,
                'lost': info['lost'],
                'inactive': info['inactive'],
                'players': info['players'],
                'dates': info['dates'],
                'matchups': affected_matchups,
            })

    if not alerts:
        print("  No roster changes affect teams with live Kalshi markets.\n")
        return

    alerts.sort(key=lambda a: -(a['lost'] + a['inactive']))

    print(f"  *** {len(alerts)} ALERT(S) ***\n")
    for a in alerts:
        total = a['lost'] + a['inactive']
        parts = []
        if a['lost']:
            parts.append(f"{a['lost']} left")
        if a['inactive']:
            parts.append(f"{a['inactive']} inactive/benched")
        status = ', '.join(parts)

        print(f"  ALERT: {a['kalshi_team']} — {total} player(s) ({status})")
        print(f"    Players: {', '.join(a['players'])}")
        print(f"    Dates:   {', '.join(a['dates'])}")
        if a['matchups']:
            for mu in a['matchups']:
                print(f"    Market:  {mu}")
        print()


def main():
    parser = argparse.ArgumentParser(description='Monitor CS2 roster changes for Kalshi-listed teams')
    parser.add_argument('--days', type=int, default=3, help='Lookback window in days (default: 3)')
    parser.add_argument('--loop', type=int, default=0, help='Re-check interval in seconds (0 = one-shot)')
    args = parser.parse_args()

    if args.loop > 0:
        while True:
            try:
                run_check(args.days)
            except KeyboardInterrupt:
                print("\n  Stopped.")
                sys.exit(0)
            except Exception as e:
                print(f"  Error: {e}")
            print(f"  Next check in {args.loop}s...")
            try:
                time.sleep(args.loop)
            except KeyboardInterrupt:
                print("\n  Stopped.")
                sys.exit(0)
    else:
        run_check(args.days)


if __name__ == '__main__':
    main()