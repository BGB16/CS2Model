"""
train_model.py — CS2 match prediction via time-decay one-hot team encoding.

Ridge regression on binary match outcomes (team won series or not).
One row per match, mirrored for symmetry.

Usage:
    python3 train_model.py                          # train and show rankings
    python3 train_model.py predict "Vitality" "FaZe"
    python3 train_model.py rankings --top 30
"""

import os, sys, argparse, pickle, warnings, math
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import OneHotEncoder
from scipy.sparse import hstack, csr_matrix
from scipy.special import expit as sigmoid

warnings.filterwarnings('ignore')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
MODEL_DIR = os.path.join(DATA_DIR, 'model')
MATCHES_FILE = os.path.join(DATA_DIR, 'matches.csv')

HALF_LIFE_DAYS = 120
ALPHA = 1.0
MIN_MATCHES = 10

BO_WEIGHT = {1: 0.6, 2: 0.8, 3: 1.0, 5: 1.2}

TEAM_RESET_DATES = {
    'Walczaki': '2026-04-18',
}


def load_matches(include_forfeits=False):
    if not os.path.exists(MATCHES_FILE):
        print(f"Error: {MATCHES_FILE} not found. Run scrape_hltv.py first.")
        sys.exit(1)
    df = pd.read_csv(MATCHES_FILE)
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['date', 'team1', 'team2'])
    if not include_forfeits and 'forfeit' in df.columns:
        df = df[df['forfeit'].fillna('').astype(str).str.len() == 0]
    df = df.sort_values('date').reset_index(drop=True)
    return df


def build_training_data(df):
    """Mirror matches into symmetric rows: one from each team's perspective."""
    reset_dates = {t: pd.Timestamp(d) for t, d in TEAM_RESET_DATES.items()}
    rows = []
    for _, m in df.iterrows():
        t1, t2, date = m['team1'], m['team2'], m['date']
        if t1 in reset_dates and date < reset_dates[t1]:
            continue
        if t2 in reset_dates and date < reset_dates[t2]:
            continue
        s1, s2 = m['team1_score'], m['team2_score']
        if m['best_of'] == 1:
            s1, s2 = (1, 0) if s1 > s2 else (0, 1)
        margin = (s1 - s2) / math.ceil(m['best_of'] / 2)
        rows.append({
            'date': date,
            'team': t1,
            'opponent': t2,
            'win': margin,
            'best_of': m['best_of'],
        })
        rows.append({
            'date': date,
            'team': t2,
            'opponent': t1,
            'win': -margin,
            'best_of': m['best_of'],
        })
    return pd.DataFrame(rows)


def build_features(df, encoders=None, fit=False):
    """One-hot encode team and opponent."""
    if encoders is None:
        encoders = {}
    for col in ['team', 'opponent']:
        vals = df[[col]].astype(str)
        if fit:
            encoders[col] = OneHotEncoder(
                sparse_output=True, handle_unknown='ignore').fit(vals)
    parts = [encoders[col].transform(df[[col]].astype(str)) for col in ['team', 'opponent']]
    X = hstack(parts, format='csr')
    return X, encoders


def train(df, half_life=None, alpha=None):
    if half_life is None:
        half_life = HALF_LIFE_DAYS
    if alpha is None:
        alpha = ALPHA

    train_df = build_training_data(df)
    print(f"  Matches: {len(df)}")
    print(f"  Training rows: {len(train_df)} ({len(df)} matches mirrored)")
    print(f"  Teams: {train_df['team'].nunique()}")
    print(f"  Date range: {train_df['date'].min().date()} to {train_df['date'].max().date()}")
    print(f"  Half-life: {half_life} days, Alpha: {alpha}")

    X, encoders = build_features(train_df, fit=True)
    y = train_df['win'].values.astype(float)

    max_date = train_df['date'].max()
    days_ago = (max_date - train_df['date']).dt.total_seconds() / 86400.0
    lam = np.log(2) / half_life
    time_weights = np.exp(-lam * days_ago.values)
    bo_weights = train_df['best_of'].map(BO_WEIGHT).fillna(0.8).values
    weights = time_weights * bo_weights

    model = Ridge(alpha=alpha, fit_intercept=True)
    model.fit(X, y, sample_weight=weights)

    preds = model.predict(X)
    win_actual = (y > 0).astype(float)
    win_pred = (preds > 0).astype(float)
    accuracy = np.mean(win_actual == win_pred)

    from scipy.optimize import minimize_scalar
    def brier_at_scale(s):
        return np.mean((sigmoid(preds * s) - win_actual) ** 2)
    result = minimize_scalar(brier_at_scale, bounds=(0.5, 10.0), method='bounded')
    scale = result.x

    probs = sigmoid(preds * scale)
    brier = np.mean((probs - win_actual) ** 2)
    mae = np.mean(np.abs(preds - y))

    print(f"\n  === Training Metrics ===")
    print(f"  Accuracy:    {accuracy:.4f}")
    print(f"  Margin MAE:  {mae:.4f}")
    print(f"  Brier score: {brier:.4f}")
    print(f"  Sigmoid scale: {scale:.3f}")

    return model, encoders, train_df, scale


def get_power_rankings(model, encoders, train_df, min_matches=None):
    if min_matches is None:
        min_matches = MIN_MATCHES

    team_encoder = encoders['team']
    team_names = team_encoder.categories_[0]
    n_teams = len(team_names)

    team_coefs = model.coef_[:n_teams]

    team_counts = train_df['team'].value_counts()
    rankings = []
    for i, name in enumerate(team_names):
        count = team_counts.get(name, 0)
        if count >= min_matches:
            rankings.append({
                'team': name,
                'rating': team_coefs[i],
                'matches': count,
            })

    rankings.sort(key=lambda x: x['rating'], reverse=True)
    return rankings


TEAM_ALIASES = {
    'navi': 'Natus Vincere',
    'na\'vi': 'Natus Vincere',
    'natus vincere': 'Natus Vincere',
    'g2': 'G2',
    'g2 esports': 'G2',
    'faze': 'FaZe',
    'faze clan': 'FaZe',
    'nip': 'Ninjas in Pyjamas',
    'c9': 'Cloud9',
    'cloud9': 'Cloud9',
    'col': 'Complexity',
    'big': 'BIG',
    'og': 'OG',
    'vp': 'Virtus.pro',
    'virtus.pro': 'Virtus.pro',
    'team spirit': 'Spirit',
    'team falcons': 'Falcons',
    'team aurora': 'Aurora',
    'the mongolz': 'The MongolZ',
    'mongolz': 'The MongolZ',
    'furia esports': 'FURIA',
    'furia': 'FURIA',
    'mouz': 'MOUZ',
    'heroic': 'HEROIC',
    'fisher college navy': 'Fisher College',
    'gentle mates': 'Gentle Mates',
    'parivision': 'PARIVISION',
    'leo': 'Leo',
}


def resolve_team_name(query, encoders):
    """Fuzzy match a team name against known teams. Returns exact name or None."""
    team_names = list(encoders['team'].categories_[0])
    query_lower = query.lower().strip()

    if query_lower in TEAM_ALIASES:
        alias_target = TEAM_ALIASES[query_lower]
        if alias_target in team_names:
            return alias_target

    for name in team_names:
        if name == query:
            return name

    for name in team_names:
        if name.lower() == query_lower:
            return name

    substring_matches = []
    for name in team_names:
        name_lower = name.lower()
        if len(query_lower) >= 4 and query_lower in name_lower:
            substring_matches.append(name)
        elif len(name_lower) >= 4 and name_lower in query_lower:
            substring_matches.append(name)
    if substring_matches:
        substring_matches.sort(key=lambda n: -len(n))
        return substring_matches[0]

    query_tokens = set(query_lower.split())
    best_score, best_name = 0, None
    for name in team_names:
        name_tokens = set(name.lower().split())
        overlap = len(query_tokens & name_tokens)
        min_tokens = min(len(query_tokens), len(name_tokens))
        if min_tokens > 0 and overlap >= max(1, min_tokens // 2 + 1):
            if overlap > best_score:
                best_score = overlap
                best_name = name

    if best_score > 0:
        return best_name

    return None


def predict_match(model, encoders, team_a, team_b, scale=1.0):
    """Predict win probability for team_a vs team_b."""
    resolved_a = resolve_team_name(team_a, encoders)
    resolved_b = resolve_team_name(team_b, encoders)

    if not resolved_a:
        print(f"  Warning: '{team_a}' not found in data")
    if not resolved_b:
        print(f"  Warning: '{team_b}' not found in data")

    use_a = resolved_a or team_a
    use_b = resolved_b or team_b

    if resolved_a and resolved_a != team_a:
        print(f"  Matched '{team_a}' -> '{resolved_a}'")
    if resolved_b and resolved_b != team_b:
        print(f"  Matched '{team_b}' -> '{resolved_b}'")

    pred_df = pd.DataFrame([{'team': use_a, 'opponent': use_b}])
    X, _ = build_features(pred_df, encoders=encoders)
    margin = float(model.predict(X)[0])
    prob = float(np.clip(sigmoid(margin * scale), 0.01, 0.99))

    return prob


FF_DECAY_HL = 14
FF_TRAIN_HL = 60
FF_DAYS_CAP = 180

def get_power_rating_map(model, encoders):
    """Extract {team_name: coefficient} from the win model."""
    team_encoder = encoders['team']
    team_names = team_encoder.categories_[0]
    team_coefs = model.coef_[:len(team_names)]
    return {name: float(team_coefs[i]) for i, name in enumerate(team_names)}


def _ff_features_from_history(hist, date, power, opp_power):
    """Compute forfeit feature vector from a team's match history up to `date`."""
    ff_7d = ff_14d = ff_30d = ff_90d = 0
    matches_7d = matches_14d = matches_30d = 0
    decay_num = 0.0
    decay_den = 0.0
    days_since_ff = FF_DAYS_CAP
    has_ever_ff = 0
    lam = np.log(2) / FF_DECAY_HL

    for h in hist:
        days = (date - h['date']).total_seconds() / 86400.0
        if days < 0:
            continue
        w = np.exp(-lam * days)
        decay_den += w
        if h['ff']:
            decay_num += w
            has_ever_ff = 1
            if days < days_since_ff:
                days_since_ff = days

        if days <= 7:
            matches_7d += 1
            if h['ff']:
                ff_7d += 1
        if days <= 14:
            matches_14d += 1
            if h['ff']:
                ff_14d += 1
        if days <= 30:
            matches_30d += 1
            if h['ff']:
                ff_30d += 1
        if days <= 90:
            if h['ff']:
                ff_90d += 1

    ff_rate_decay = decay_num / decay_den if decay_den > 0 else 0.0
    log_days_since = np.log1p(days_since_ff)

    return {
        'power_rating': power,
        'opp_power_rating': opp_power,
        'rating_diff': power - opp_power,
        'ff_7d': ff_7d,
        'ff_14d': ff_14d,
        'ff_30d': ff_30d,
        'ff_90d': ff_90d,
        'ff_rate_decay': ff_rate_decay,
        'days_since_ff': days_since_ff,
        'log_days_since_ff': log_days_since,
        'matches_7d': matches_7d,
        'matches_14d': matches_14d,
        'matches_30d': matches_30d,
        'has_ever_ff': has_ever_ff,
    }


FF_FEATURES = [
    'power_rating', 'opp_power_rating', 'rating_diff',
    'ff_7d', 'ff_14d', 'ff_30d', 'ff_90d',
    'ff_rate_decay', 'log_days_since_ff',
    'matches_7d', 'matches_14d', 'matches_30d',
    'has_ever_ff',
]


def build_forfeit_dataset(all_df, power_map):
    """Build forfeit training rows chronologically. Features use only prior history."""
    all_df = all_df.sort_values('date').reset_index(drop=True)
    ff_col = all_df['forfeit'].fillna('').astype(str) if 'forfeit' in all_df.columns else pd.Series([''] * len(all_df))

    team_history = {}
    rows = []

    for idx, m in all_df.iterrows():
        date = m['date']
        t1, t2 = m['team1'], m['team2']
        ff_team = ff_col.iloc[idx]
        is_forfeit = len(ff_team) > 0

        for team, opponent in [(t1, t2), (t2, t1)]:
            hist = team_history.get(team, [])
            power = power_map.get(team, 0.0)
            opp_power = power_map.get(opponent, 0.0)

            feats = _ff_features_from_history(hist, date, power, opp_power)
            feats['date'] = date
            feats['team'] = team
            feats['opponent'] = opponent
            feats['forfeited'] = 1 if (is_forfeit and ff_team == team) else 0

            rows.append(feats)

            if team not in team_history:
                team_history[team] = []
            team_history[team].append({
                'date': date,
                'ff': is_forfeit and ff_team == team,
            })

    return pd.DataFrame(rows)


def train_forfeit_model(all_df, power_map):
    """Train gradient-boosted classifier to predict P(forfeit)."""
    ff_df = build_forfeit_dataset(all_df, power_map)

    min_history = ff_df[ff_df['matches_14d'] >= 1]
    if len(min_history) < 20:
        print("  Not enough data for forfeit model")
        return None

    X = min_history[FF_FEATURES].values
    y = min_history['forfeited'].values

    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    print(f"\n  === Forfeit Model Training ===")
    print(f"  Rows: {len(min_history)} ({n_pos} forfeits, {n_neg} normal)")
    print(f"  Forfeit base rate: {n_pos / len(y):.4f}")

    if n_pos < 3:
        print("  Too few forfeits to train — skipping")
        return None

    max_date = min_history['date'].max()
    days_ago = (max_date - min_history['date']).dt.total_seconds() / 86400.0
    lam = np.log(2) / FF_TRAIN_HL
    sample_weights = np.exp(-lam * days_ago.values)

    scale_pos_weight = np.sqrt(n_neg / n_pos)

    ff_model = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=2,
        learning_rate=0.03,
        min_samples_leaf=max(10, n_pos // 2),
        subsample=0.8,
    )

    sw = sample_weights.copy()
    pos_mask = y == 1
    sw[pos_mask] *= scale_pos_weight
    ff_model.fit(X, y, sample_weight=sw)

    raw_probs = ff_model.predict_proba(X)[:, 1]

    from sklearn.isotonic import IsotonicRegression
    calibrator = IsotonicRegression(y_min=0, y_max=1, out_of_bounds='clip')
    calibrator.fit(raw_probs, y)
    probs = calibrator.predict(raw_probs)

    pred_labels = (probs >= 0.10).astype(int)

    from sklearn.metrics import precision_score, recall_score
    prec = precision_score(y, pred_labels, zero_division=0)
    recall = recall_score(y, pred_labels, zero_division=0)
    brier = np.mean((probs - y) ** 2)
    avg_ff_prob = probs[pos_mask].mean() if n_pos > 0 else 0
    avg_norm_prob = probs[~pos_mask].mean()

    print(f"\n  Metrics (threshold=10%):")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  Brier:     {brier:.6f}")
    print(f"  Avg P(ff) on forfeits:  {avg_ff_prob:.4f}")
    print(f"  Avg P(ff) on normal:    {avg_norm_prob:.4f}")
    print(f"  Separation ratio:       {avg_ff_prob / max(avg_norm_prob, 1e-6):.1f}x")

    importances = dict(zip(FF_FEATURES, ff_model.feature_importances_))
    print(f"\n  Feature importance:")
    for feat, imp in sorted(importances.items(), key=lambda x: -x[1]):
        if imp >= 0.01:
            print(f"    {feat:<22} {imp:.3f}")

    return {'model': ff_model, 'calibrator': calibrator, 'features': FF_FEATURES}


def predict_forfeit(ff_model_data, team, opponent, power_map, all_df):
    """Predict P(forfeit) for a team using current match history."""
    if ff_model_data is None:
        return 0.0

    ff_model = ff_model_data['model']
    calibrator = ff_model_data.get('calibrator')
    all_df = all_df.sort_values('date').reset_index(drop=True)
    ff_col = all_df['forfeit'].fillna('').astype(str) if 'forfeit' in all_df.columns else pd.Series([''] * len(all_df))

    now = pd.Timestamp.now()
    team_mask = (all_df['team1'] == team) | (all_df['team2'] == team)
    team_rows = all_df[team_mask]

    hist = []
    for i, row in team_rows.iterrows():
        hist.append({
            'date': row['date'],
            'ff': ff_col.iloc[i] == team,
        })

    power = power_map.get(team, 0.0)
    opp_power = power_map.get(opponent, 0.0)

    feats = _ff_features_from_history(hist, now, power, opp_power)
    feature_vec = np.array([[feats[f] for f in FF_FEATURES]])
    raw = float(ff_model.predict_proba(feature_vec)[:, 1][0])
    if calibrator is not None:
        return float(calibrator.predict([raw])[0])
    return raw


def save_model(model, encoders, scale, ff_model_data=None):
    os.makedirs(MODEL_DIR, exist_ok=True)
    payload = {'model': model, 'encoders': encoders, 'scale': scale}
    if ff_model_data is not None:
        payload['ff_model'] = ff_model_data
    with open(os.path.join(MODEL_DIR, 'model.pkl'), 'wb') as f:
        pickle.dump(payload, f)
    print(f"  Model saved to {MODEL_DIR}/model.pkl")


def load_model():
    path = os.path.join(MODEL_DIR, 'model.pkl')
    if not os.path.exists(path):
        print(f"Error: {path} not found. Run train first.")
        sys.exit(1)
    with open(path, 'rb') as f:
        data = pickle.load(f)
    return data['model'], data['encoders'], data.get('scale', 1.0)


def load_forfeit_model():
    path = os.path.join(MODEL_DIR, 'model.pkl')
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as f:
        data = pickle.load(f)
    return data.get('ff_model')


def main():
    parser = argparse.ArgumentParser(description="CS2 match prediction model")
    parser.add_argument('command', nargs='?', default='train',
                        choices=['train', 'predict', 'rankings', 'forfeit'],
                        help='Command to run')
    parser.add_argument('teams', nargs='*', help='Teams for predict command')
    parser.add_argument('--top', type=int, default=0, help='Top N for rankings (0 = all)')
    parser.add_argument('--half-life', type=int, default=HALF_LIFE_DAYS,
                        help='Time decay half-life in days')
    parser.add_argument('--alpha', type=float, default=ALPHA,
                        help='Ridge regularization strength')
    parser.add_argument('--min-matches', type=int, default=MIN_MATCHES,
                        help='Minimum matches for rankings')
    args = parser.parse_args()

    if args.command == 'train':
        df = load_matches()
        print(f"\n  === CS2 Model Training ===")
        model, encoders, train_df, scale = train(df, half_life=args.half_life, alpha=args.alpha)

        rankings = get_power_rankings(model, encoders, train_df, min_matches=args.min_matches)
        show = rankings if args.top == 0 else rankings[:args.top]
        print(f"\n  === Power Rankings ({len(show)} teams, {args.min_matches}+ matches) ===")
        print(f"  {'Rank':<6}{'Team':<25}{'Rating':<10}  {'Matches'}")
        print(f"  {'-'*50}")
        for i, r in enumerate(show, 1):
            print(f"  {i:<6}{r['team']:<25}{r['rating']:>+.4f}    {r['matches']}")

        ff_model_data = None
        all_df = load_matches(include_forfeits=True)
        if 'forfeit' in all_df.columns and all_df['forfeit'].fillna('').astype(str).str.len().gt(0).any():
            power_map = get_power_rating_map(model, encoders)
            ff_model_data = train_forfeit_model(all_df, power_map)

        save_model(model, encoders, scale, ff_model_data)

    elif args.command == 'predict':
        if len(args.teams) < 2:
            print("Usage: python3 train_model.py predict \"Team A\" \"Team B\"")
            sys.exit(1)
        team_a, team_b = args.teams[0], args.teams[1]
        model, encoders, scale = load_model()
        df = load_matches()
        train_df = build_training_data(df)
        match_counts = train_df['team'].value_counts().to_dict()
        match_counts_lower = {k.lower(): v for k, v in match_counts.items()}
        prob_a = predict_match(model, encoders, team_a, team_b, scale)
        prob_b = 1 - prob_a
        resolved_a = resolve_team_name(team_a, encoders) or team_a
        resolved_b = resolve_team_name(team_b, encoders) or team_b
        count_a = match_counts.get(resolved_a) or match_counts_lower.get(resolved_a.lower(), 0)
        count_b = match_counts.get(resolved_b) or match_counts_lower.get(resolved_b.lower(), 0)
        print(f"\n  {resolved_a} ({count_a} matches)"
              f" vs {resolved_b} ({count_b} matches)")
        print(f"  {resolved_a}: {prob_a:.1%}")
        print(f"  {resolved_b}: {prob_b:.1%}")

    elif args.command == 'rankings':
        model, encoders, scale = load_model()
        df = load_matches()
        train_df = build_training_data(df)
        rankings = get_power_rankings(model, encoders, train_df, min_matches=args.min_matches)
        show = rankings if args.top == 0 else rankings[:args.top]
        print(f"\n  === Power Rankings ({len(show)} teams, {args.min_matches}+ matches) ===")
        print(f"  {'Rank':<6}{'Team':<25}{'Rating':<10}  {'Matches'}")
        print(f"  {'-'*50}")
        for i, r in enumerate(show, 1):
            print(f"  {i:<6}{r['team']:<25}{r['rating']:>+.4f}    {r['matches']}")

    elif args.command == 'forfeit':
        model, encoders, scale = load_model()
        ff_model_data = load_forfeit_model()
        if not ff_model_data:
            print("  No forfeit model found. Run 'train' first with forfeit data.")
            sys.exit(1)
        power_map = get_power_rating_map(model, encoders)
        all_df = load_matches(include_forfeits=True)

        if args.teams:
            for team_q in args.teams:
                resolved = resolve_team_name(team_q, encoders) or team_q
                opp = args.teams[1] if len(args.teams) > 1 and team_q == args.teams[0] else 'Average'
                opp_resolved = resolve_team_name(opp, encoders) if opp != 'Average' else None
                prob = predict_forfeit(ff_model_data, resolved, opp_resolved or resolved, power_map, all_df)
                print(f"  {resolved}: P(forfeit) = {prob:.2%}  (power: {power_map.get(resolved, 0):.4f})")
        else:
            print(f"\n  === Forfeit Risk Rankings ===")
            print(f"  {'Team':<25}{'P(FF)':<10}{'Power':<10}  {'90d FF'}")
            print(f"  {'-'*55}")
            teams = list(encoders['team'].categories_[0])
            risks = []
            for team in teams:
                prob = predict_forfeit(ff_model_data, team, team, power_map, all_df)
                risks.append((team, prob, power_map.get(team, 0)))
            risks.sort(key=lambda x: -x[1])
            show = risks[:args.top] if args.top > 0 else [r for r in risks if r[1] >= 0.01]
            for team, prob, power in show:
                ff_col = all_df['forfeit'].fillna('').astype(str)
                ff_90 = ((ff_col == team) & (all_df['date'] >= pd.Timestamp.now() - pd.Timedelta(days=90))).sum()
                print(f"  {team:<25}{prob:<10.2%}{power:<+10.4f}  {ff_90}")


if __name__ == '__main__':
    main()
