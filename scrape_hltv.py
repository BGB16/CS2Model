"""
scrape_hltv.py — Backfill CS2 match results from hltv.org/results.

Scrapes completed match results page-by-page (100 per page via offset).
One row per series (match), with series map scores (e.g. 2-0, 2-1).

Uses: https://www.hltv.org/results?offset=N

Usage:
    python3 scrape_hltv.py                             # scrape last year
    python3 scrape_hltv.py --pages 5                   # just 5 pages
    python3 scrape_hltv.py --start 2024-01-01          # go back to this date
    python3 scrape_hltv.py --end 2026-04-01            # only keep up to this date
    python3 scrape_hltv.py --start 2025-01-01 --end 2025-06-01
    python3 scrape_hltv.py --check                     # show data status
    python3 scrape_hltv.py --reset                     # delete all scraped data
"""

import os, csv, sys, time, json, re, argparse
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
MATCHES_FILE = os.path.join(DATA_DIR, "matches.csv")
PROGRESS_FILE = os.path.join(DATA_DIR, "scrape_progress.json")

BASE_URL = "https://www.hltv.org/results"
RESULTS_PER_PAGE = 100

MATCH_COLUMNS = [
    'date', 'unix_ts', 'team1', 'team2',
    'team1_score', 'team2_score',
    'best_of', 'forfeit', 'event', 'match_url',
]


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_offset": 0, "total_scraped": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def load_existing_urls():
    if not os.path.exists(MATCHES_FILE):
        return set()
    with open(MATCHES_FILE) as f:
        reader = csv.DictReader(f)
        return {row['match_url'] for row in reader}


def check_data():
    if not os.path.exists(MATCHES_FILE):
        print("  No data scraped yet.")
        return
    import pandas as pd
    df = pd.read_csv(MATCHES_FILE)
    print(f"\n  === CS2 HLTV Data Status ===")
    print(f"  Total matches: {len(df)}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Unique teams: {len(set(df['team1'].unique()) | set(df['team2'].unique()))}")
    print(f"  Unique events: {df['event'].nunique()}")
    print(f"\n  Format breakdown:")
    for bo, count in sorted(df['best_of'].value_counts().items()):
        label = 'forfeit' if bo == 0 else f'bo{bo}'
        print(f"    {label}: {count}")
    if 'forfeit' in df.columns:
        ff = df[df['forfeit'].fillna('').astype(str).str.len() > 0]
        if len(ff):
            print(f"\n  Top forfeiting teams (last year):")
            all_ff = ff['forfeit'].value_counts().head(15)
            for team, cnt in all_ff.items():
                print(f"    {team}: {cnt}")
    progress = load_progress()
    print(f"\n  Last offset: {progress.get('last_offset', 0)}")
    print(f"  Total scraped: {progress.get('total_scraped', 0)}")


def parse_date_headline(headline_text):
    """Parse 'Results for May 5th 2026' -> '2026-05-05'"""
    text = headline_text.replace("Results for ", "").strip()
    text = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', text)
    try:
        dt = datetime.strptime(text, "%B %d %Y")
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return text


def parse_best_of(map_text):
    """Parse 'bo3' -> 3, 'bo1' -> 1, 'bo5' -> 5, 'def' -> 0."""
    map_text = map_text.strip().lower()
    if map_text == 'def':
        return 0
    m = re.match(r'bo(\d+)', map_text)
    if m:
        return int(m.group(1))
    return 1


def parse_results_page(html):
    """Parse a single HLTV results page, returning list of match dicts."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    matches = []
    # HLTV has a featured results-holder (no dates) followed by the main one
    all_holders = soup.find_all('div', class_='results-holder')
    results_holder = all_holders[-1] if all_holders else None
    if not results_holder:
        return matches

    sublists = results_holder.find_all('div', class_='results-sublist')

    headline_dates = []
    for sublist in sublists:
        headline = sublist.find(class_='standard-headline')
        if headline:
            headline_dates.append(parse_date_headline(headline.get_text(strip=True)))
        else:
            headline_dates.append(None)

    for idx, sublist in enumerate(sublists):
        current_date = headline_dates[idx]
        if not current_date:
            for later_idx in range(idx + 1, len(headline_dates)):
                if headline_dates[later_idx]:
                    current_date = headline_dates[later_idx]
                    break

        result_cons = sublist.find_all('div', class_='result-con')
        for con in result_cons:
            try:
                unix_ts = con.get('data-zonedgrouping-entry-unix', '')

                link = con.find('a', class_='a-reset')
                match_url = link.get('href', '') if link else ''

                team1_div = con.find('div', class_='line-align team1')
                team2_div = con.find('div', class_='line-align team2')
                if not team1_div or not team2_div:
                    continue

                team1_name_div = team1_div.find('div', class_='team')
                team2_name_div = team2_div.find('div', class_='team')
                if not team1_name_div or not team2_name_div:
                    continue

                team1 = team1_name_div.get_text(strip=True)
                team2 = team2_name_div.get_text(strip=True)

                score_td = con.find('td', class_='result-score')
                if not score_td:
                    continue
                scores = score_td.find_all('span')
                if len(scores) < 2:
                    continue

                score1 = int(scores[0].get_text(strip=True))
                score2 = int(scores[1].get_text(strip=True))

                map_div = con.find('div', class_='map-text')
                best_of = 0
                if map_div:
                    best_of = parse_best_of(map_div.get_text(strip=True))

                event_td = con.find('td', class_='event')
                event_name = ''
                if event_td:
                    event_span = event_td.find('span', class_='event-name')
                    if event_span:
                        event_name = event_span.get_text(strip=True)

                match_date = current_date
                if not match_date and unix_ts:
                    try:
                        ts_sec = int(unix_ts) / 1000
                        match_date = datetime.fromtimestamp(ts_sec).strftime('%Y-%m-%d')
                    except (ValueError, OSError):
                        pass

                forfeit_team = ''
                if best_of == 0:
                    forfeit_team = team2 if score1 > score2 else team1

                matches.append({
                    'date': match_date,
                    'unix_ts': unix_ts,
                    'team1': team1,
                    'team2': team2,
                    'team1_score': score1,
                    'team2_score': score2,
                    'best_of': best_of,
                    'forfeit': forfeit_team,
                    'event': event_name,
                    'match_url': match_url,
                })
            except Exception:
                continue

    return matches


def append_matches(rows):
    """Append rows to matches.csv."""
    file_exists = os.path.exists(MATCHES_FILE)
    with open(MATCHES_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=MATCH_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            filtered = {k: row.get(k, '') for k in MATCH_COLUMNS}
            writer.writerow(filtered)



def scrape(max_pages=None, start_date=None, end_date=None, resume=True):
    """Scrape HLTV results by paginating offset."""
    from hltv_map_monitor import get_hltv_browser, close_hltv_browser

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    progress = load_progress() if resume else {"last_offset": 0, "total_scraped": 0}
    existing_urls = load_existing_urls()
    start_offset = 0 if existing_urls else (progress["last_offset"] if resume else 0)

    print(f"\n  === Scraping HLTV Results ===")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Starting at offset: {start_offset}")
    print(f"  Existing match URLs: {len(existing_urls)}")

    print("  Starting browser...", end=" ", flush=True)
    browser = get_hltv_browser()
    print("done.")

    page_num = 0
    offset = start_offset
    total_new = 0
    consecutive_failures = 0
    consecutive_no_new = 0
    reached_start = False

    try:
        while True:
            if max_pages and page_num >= max_pages:
                break
            if reached_start:
                break

            url = f"{BASE_URL}?offset={offset}" if offset > 0 else BASE_URL
            print(f"\n  Page {page_num + 1} (offset={offset})...", end=" ", flush=True)

            try:
                html = browser.get_page_html(url, timeout=30000)
                consecutive_failures = 0
            except Exception as e:
                print(f"FAILED: {e}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    print("  3 consecutive failures, stopping.")
                    break
                time.sleep(10)
                continue

            matches = parse_results_page(html)
            if not matches:
                print("no results found, stopping.")
                break

            page_dates = [m['date'] for m in matches if m['date']]
            if page_dates:
                oldest_on_page = min(page_dates)
                if oldest_on_page < start_date:
                    reached_start = True

            matches = [
                m for m in matches
                if m['date'] and m['date'] >= start_date and m['date'] <= end_date
            ]

            new_matches = [m for m in matches if m['match_url'] not in existing_urls]

            valid_matches = [
                m for m in new_matches
                if '1vs1' not in m['event'].lower()
                and '1v1' not in m['event'].lower()
                and m['team1'].lower() != 'home'
                and m['team2'].lower() != 'home'
            ]

            forfeits_kept = len([m for m in valid_matches if m['best_of'] == 0])
            skipped_1v1 = len([m for m in new_matches
                              if '1vs1' in m['event'].lower() or '1v1' in m['event'].lower()])
            print(f"{len(matches)} in range, {len(valid_matches)} new"
                  f"{f', {forfeits_kept} ff' if forfeits_kept else ''}"
                  f"{f', {skipped_1v1} 1v1' if skipped_1v1 else ''}"
                  f"{' [REACHED START DATE]' if reached_start else ''}")

            if valid_matches:
                append_matches(valid_matches)
                for m in valid_matches:
                    existing_urls.add(m['match_url'])
                total_new += len(valid_matches)
                consecutive_no_new = 0
            else:
                consecutive_no_new += 1

            if existing_urls and consecutive_no_new >= 3 and not reached_start:
                print("  3 pages with no new matches — caught up, stopping.")
                break

            offset += RESULTS_PER_PAGE
            page_num += 1

            progress["last_offset"] = offset
            progress["total_scraped"] = progress.get("total_scraped", 0) + len(valid_matches)
            save_progress(progress)

            delay = 2.5 + (page_num % 3) * 0.7
            time.sleep(delay)

    finally:
        close_hltv_browser()

    print(f"\n  Done! {total_new} new matches scraped.")
    print(f"  Total match URLs in dataset: {len(existing_urls)}")
    save_progress(progress)


def main():
    parser = argparse.ArgumentParser(description="Scrape HLTV CS2 match results")
    parser.add_argument('--pages', type=int, default=None,
                        help='Max pages to scrape (100 results each)')
    parser.add_argument('--start', type=str, default=None,
                        help='Earliest date to include (YYYY-MM-DD), default: 1 year ago')
    parser.add_argument('--end', type=str, default=None,
                        help='Latest date to include (YYYY-MM-DD), default: today')
    parser.add_argument('--check', action='store_true', help='Show data status')
    parser.add_argument('--reset', action='store_true', help='Delete all scraped data')
    parser.add_argument('--no-resume', action='store_true',
                        help='Start from offset 0 instead of resuming')
    args = parser.parse_args()

    if args.check:
        check_data()
        return

    if args.reset:
        for f in [MATCHES_FILE, PROGRESS_FILE]:
            if os.path.exists(f):
                os.remove(f)
                print(f"  Removed {f}")
        print("  Data reset complete.")
        return

    os.makedirs(DATA_DIR, exist_ok=True)

    scrape(max_pages=args.pages, start_date=args.start, end_date=args.end,
           resume=not args.no_resume)


if __name__ == '__main__':
    main()
