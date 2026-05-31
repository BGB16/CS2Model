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
from poly_cs2 import PolyClient, get_poly_orderbook, get_best_ask as poly_best_ask, GAMMA_BASE

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

API_KEY_ID = None
PRIVATE_KEY = None
DRY_RUN = False
CONTRACTS = 50
SPREAD_CENTS = 6

PLACED_ORDER_IDS = []
PLACED_ORDER_LOCK = threading.Lock()

POLY_CLIENT = None
FUTURES_TOKEN_IDS = []
FUTURES_TOKEN_LOCK = threading.Lock()

app = Flask(__name__)


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
                               bomb_planted=False, timer_seconds=-1):
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
            # Project next-round money for each player.
            # Winners get $3250, losers get loss bonus ($1400-$3400 based on streak).
            # Estimate loss streak from recent economy trajectory:
            # if the losing team had high avg money, they just lost a buy (streak=1).
            # if they had low money, they've been losing (higher streak).
            def _est_loss_bonus(loser_avg, winner_avg):
                """Estimate loss bonus from the loser's current avg money."""
                # High money -> just lost first buy -> low streak
                # Mid money -> couple losses -> mid streak
                # Very low money -> long streak or just lost after eco
                if loser_avg >= 3000:
                    return LOSS_BONUS[0]  # $1400 — first loss, had money
                elif loser_avg >= 2000:
                    return LOSS_BONUS[1]  # $1900
                elif loser_avg >= 1000:
                    return LOSS_BONUS[2]  # $2400
                else:
                    return LOSS_BONUS[3]  # $2900 — been losing a while

            ct_projected = []
            for p in ct_players:
                m = p.get('money', 0)
                if ct_won:
                    m += WIN_REWARD
                else:
                    m += _est_loss_bonus(ct_avg, t_avg)
                ct_projected.append(min(MAX_MONEY, m))

            t_projected = []
            for p in t_players:
                m = p.get('money', 0)
                if not ct_won:
                    m += WIN_REWARD
                    if bomb_planted:
                        m += PLANT_BONUS
                else:
                    m += _est_loss_bonus(t_avg, ct_avg)
                    if bomb_planted:
                        m += PLANT_BONUS
                t_projected.append(min(MAX_MONEY, m))

            ct_avg = sum(ct_projected) / max(len(ct_projected), 1)
            t_avg = sum(t_projected) / max(len(t_projected), 1)
            ct_display_money = int(ct_avg)
            t_display_money = int(t_avg)

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

    # Economy-driven multi-round lookahead.
    # One round of EV barely moves the map price. But a round's outcome
    # determines the economy for the NEXT round, which determines buy levels.
    # Project forward 2 rounds using economy to estimate buy matchups.
    p_map_if_win = live_round_win_prob(home_rounds + 1, away_rounds, base_map_prob)
    p_map_if_lose = live_round_win_prob(home_rounds, away_rounds + 1, base_map_prob)
    expected_map_p = home_round_p * p_map_if_win + (1 - home_round_p) * p_map_if_lose

    if phase == 'live' and total_rounds < 24:
        home_info_l = ct_info if home_is_ct else t_info
        away_info_l = t_info if home_is_ct else ct_info
        home_money = home_info_l['avg_money']
        away_money = away_info_l['avg_money']
        home_equip = home_info_l['total_value']
        away_equip = away_info_l['total_value']

        def _project_next_round_buy(winner_money, winner_equip, loser_money, loss_streak):
            """Estimate next-round buy level after win/loss."""
            # Winner: gets $3250 + keeps equipment
            w_next = min(MAX_MONEY, winner_money + WIN_REWARD)
            # Loser: gets loss bonus, loses equipment (weapons gone)
            l_bonus = LOSS_BONUS[min(loss_streak, len(LOSS_BONUS) - 1)]
            l_next = min(MAX_MONEY, loser_money + l_bonus)
            w_buy = 'full' if w_next >= 3900 else ('force' if w_next >= 2400 else 'eco')
            l_buy = 'full' if l_next >= 3900 else ('force' if l_next >= 2400 else 'eco')
            return w_buy, l_buy

        # Estimate current loss streaks from money levels
        home_loss_streak = 0 if home_money > 3500 else (1 if home_money > 2000 else 2)
        away_loss_streak = 0 if away_money > 3500 else (1 if away_money > 2000 else 2)

        # If home wins this round: home is winner, away loses
        hw_buy, al_buy = _project_next_round_buy(
            home_money, home_equip, away_money, away_loss_streak + 1)
        # If home loses: away is winner, home loses
        aw_buy, hl_buy = _project_next_round_buy(
            away_money, away_equip, home_money, home_loss_streak + 1)

        # Convert projected buys to round-2 win probability
        r2_if_won = BUY_MATCHUP_RATES.get((hw_buy, al_buy) if home_is_ct else (al_buy, hw_buy), 0.50)
        if not home_is_ct:
            r2_if_won = 1 - r2_if_won
        r2_if_lost = BUY_MATCHUP_RATES.get((hl_buy, aw_buy) if home_is_ct else (aw_buy, hl_buy), 0.50)
        if not home_is_ct:
            r2_if_lost = 1 - r2_if_lost
        # Blend with skill
        r2_home_p_if_won = 0.5 * r2_if_won + 0.5 * (base_map_prob if home_is_ct else 1 - base_map_prob)
        r2_home_p_if_lost = 0.5 * r2_if_lost + 0.5 * (base_map_prob if home_is_ct else 1 - base_map_prob)

        # 2-round lookahead: for each path (win/lose this round), project next round
        # Path A: home wins R1, then R2 outcome
        p_map_win_win = live_round_win_prob(home_rounds + 2, away_rounds, base_map_prob)
        p_map_win_lose = live_round_win_prob(home_rounds + 1, away_rounds + 1, base_map_prob)
        p_map_after_win = r2_home_p_if_won * p_map_win_win + (1 - r2_home_p_if_won) * p_map_win_lose

        # Path B: home loses R1, then R2 outcome
        p_map_lose_win = live_round_win_prob(home_rounds + 1, away_rounds + 1, base_map_prob)
        p_map_lose_lose = live_round_win_prob(home_rounds, away_rounds + 2, base_map_prob)
        p_map_after_lose = r2_home_p_if_lost * p_map_lose_win + (1 - r2_home_p_if_lost) * p_map_lose_lose

        expected_map_p_2 = home_round_p * p_map_after_win + (1 - home_round_p) * p_map_after_lose

        # Use the 2-round lookahead when the situation is lopsided
        round_certainty = abs(home_round_p - 0.50) * 2
        if round_certainty > 0.20:
            blend = min(0.80, round_certainty)
            expected_map_p = (1 - blend) * expected_map_p + blend * expected_map_p_2

    home_info = ct_info if home_is_ct else t_info
    away_info = t_info if home_is_ct else ct_info

    home_alive = ct_alive if home_is_ct else t_alive
    away_alive = t_alive if home_is_ct else ct_alive

    econ_detail = {
        'home_buy': ct_buy if home_is_ct else t_buy,
        'away_buy': t_buy if home_is_ct else ct_buy,
        'home_avg_money': ct_display_money if home_is_ct else t_display_money,
        'away_avg_money': t_display_money if home_is_ct else ct_display_money,
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
    }

    return expected_map_p, econ_detail


# ── Probability ──────────────────────────────────────────────────────

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


def live_round_win_prob(team1_rounds, team2_rounds, base_map_prob):
    """Live map win probability given round score.
    Regulation: MR12, first to 13 wins. If 12-12 → overtime.
    OT: MR3 sets (6 rounds), win by 2. If 3-3 in OT set, new set starts.
    Derives per-round win rate from base_map_prob via brentq."""
    if team1_rounds == 0 and team2_rounds == 0:
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

    def _is_map_won(h, a):
        """Check if score h-a means team1 won the map."""
        if h <= 12 and a <= 12:
            return h >= 13  # impossible in regulation <=12
        # Regulation win: 13-x where x <= 11
        if h >= 13 and a <= 11:
            return True
        if a >= 13 and h <= 11:
            return False
        # OT: both >= 12. Win requires >= 16 AND lead >= 2
        # (each OT set adds up to 6 rounds from a 12-12 / 15-15 / 18-18 base)
        if h >= 13 and a >= 12:
            if h - a >= 2 and (h - 12) % 3 == 0 and (a - 12) % 3 == 0:
                return True  # clean OT set win at boundary
            if h - a >= 2 and h >= 16:
                # Check if at an OT set boundary
                ot_h, ot_a = h - 12, a - 12
                # OT sets are 6 rounds from the tie point
                # set boundaries at total OT rounds = 6, 12, 18...
                total_ot = ot_h + ot_a
                # Win at any point with +2 lead at set end or during sudden-death
                return True
        return None  # not decided

    def _from_state(h, a):
        # Regulation: not yet 12-12
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
                    return _ot_win_prob(p_round)
                if (rh, ra) in memo:
                    return memo[(rh, ra)]
                memo[(rh, ra)] = (p_round * _reg(rh + 1, ra) +
                                  (1 - p_round) * _reg(rh, ra + 1))
                return memo[(rh, ra)]

            return _reg(h, a)

        # In OT: both >= 12
        if h == 12 and a == 12:
            return _ot_win_prob(p_round)

        ot_h = h - 12
        ot_a = a - 12
        # Strip completed OT sets (3-3 ties restart)
        rh, ra = ot_h, ot_a
        while rh >= 3 and ra >= 3:
            rh -= 3
            ra -= 3
        # rh, ra = position in current OT set (each set is 6 rounds, MR3)

        ot_memo = {}

        def _ot_rec(oh, oa):
            # Win conditions: 4+ wins with 2+ lead, checked at set end
            # Set is 6 rounds: after all 6, if not decided, 3-3 resets
            if oh + oa >= 6:
                diff = oh - oa
                if diff >= 2:
                    return 1.0
                if diff <= -2:
                    return 0.0
                # 3-3 tie: new OT set with same per-round prob
                return _ot_win_prob(p_round)
            # Early clinch: can't lose even if opponent wins remaining
            remaining = 6 - oh - oa
            if oh - oa > remaining:
                return 1.0  # opponent can't catch up
            if oa - oh > remaining:
                return 0.0
            if (oh, oa) in ot_memo:
                return ot_memo[(oh, oa)]
            ot_memo[(oh, oa)] = (p_round * _ot_rec(oh + 1, oa) +
                                 (1 - p_round) * _ot_rec(oh, oa + 1))
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

    path = '/trade-api/v2/portfolio/orders'
    url = KALSHI_BASE + '/portfolio/orders'
    order = {
        'ticker': ticker, 'action': 'buy', 'side': side,
        'count': count, 'type': 'limit', 'yes_price': yes_price,
        'client_order_id': f"cs2-live-{uuid.uuid4()}",
    }
    headers = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'POST', path)
    try:
        resp = http_requests.post(url, headers=headers, json=order, timeout=10)
    except Exception as e:
        print(f"      FAILED: {e}")
        return False
    if resp.status_code == 201:
        oid = resp.json().get('order', {}).get('order_id', '?')
        print(f"      ORDER {oid} {side.upper()} @ {price_cents}c")
        with PLACED_ORDER_LOCK:
            PLACED_ORDER_IDS.append({
                'oid': oid, 'ticker': ticker,
                'side': side, 'yes_price': yes_price,
            })
        return True
    print(f"      FAILED: {resp.status_code} {resp.text[:200]}")
    return False


def place_ioc_order(ticker, side, count, price_cents):
    if DRY_RUN:
        print(f"      [DRY RUN] IOC {count} {side.upper()} @ {price_cents}c")
        return 0, count

    path = '/trade-api/v2/portfolio/orders'
    url = KALSHI_BASE + '/portfolio/orders'
    yes_price = price_cents if side == 'yes' else 100 - price_cents
    order = {
        'ticker': ticker, 'action': 'buy', 'side': side,
        'count': count, 'type': 'limit', 'yes_price': yes_price,
        'client_order_id': f"cs2-ioc-{uuid.uuid4()}",
        'time_in_force': 'immediate_or_cancel',
    }
    headers = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'POST', path)
    try:
        resp = http_requests.post(url, headers=headers, json=order, timeout=10)
    except Exception as e:
        print(f"      IOC FAILED: {e}")
        return 0, count
    if resp.status_code == 201:
        od = resp.json().get('order', {})
        filled = int(float(od.get('fill_count_fp', od.get('fill_count', 0)) or 0))
        if filled > 0:
            print(f"      IOC FILLED {filled}/{count} @ {price_cents}c")
        else:
            print(f"      IOC NOT FILLED @ {price_cents}c")
        return filled, count
    print(f"      IOC FAILED: {resp.status_code} {resp.text[:200]}")
    return 0, count


def _cancel_single_order(oid):
    for attempt in range(3):
        path = f'/trade-api/v2/portfolio/orders/{oid}'
        url = KALSHI_BASE + f'/portfolio/orders/{oid}'
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
    """Cancel all resting orders concurrently."""
    if DRY_RUN:
        with PLACED_ORDER_LOCK:
            PLACED_ORDER_IDS.clear()
        return 0, "DRY RUN — orders cleared"

    with PLACED_ORDER_LOCK:
        PLACED_ORDER_IDS.clear()

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

    print(f"[Cancel] {len(all_oids)} orders across {len(tickers)} tickers...")
    cancelled = 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in as_completed({pool.submit(_cancel_single_order, oid): oid for oid in all_oids}):
            if f.result():
                cancelled += 1

    msg = f"Cancelled {cancelled}/{len(all_oids)} orders"
    print(f"[Cancel] {msg}")
    return 1, msg


def post_live_orders(tickers_list, home_fair_cents, contracts, spread_cents,
                     home_name='HOME', away_name='AWAY'):
    """Post maker-only limit orders. Caps price at best_ask - 1 to never cross."""
    away_fair_cents = 100 - home_fair_cents
    home_bid = home_fair_cents - spread_cents
    away_bid = away_fair_cents - spread_cents

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

        if yes_bid > yes_fair:
            yes_bid = yes_fair
        if no_bid > no_fair:
            no_bid = no_fair

        # Fetch orderbook to cap at best_ask - 1 (maker only)
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

        if 1 <= yes_bid <= 99:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_bid,
                'team': f"{yes_team} YES", 'fair': yes_fair,
            })
        if 1 <= no_bid <= 99:
            orders_to_place.append({
                'ticker': ticker, 'side': 'no', 'price': no_bid,
                'team': f"{no_team} NO", 'fair': no_fair,
            })

    print(f"[Post] Firing {len(orders_to_place)} orders | "
          f"{home_name} fair={home_fair_cents}c bid={home_bid}c | "
          f"{away_name} fair={away_fair_cents}c bid={away_bid}c")

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


def post_ioc_orders(tickers_list, home_fair_cents, contracts, spread_cents,
                    home_name='HOME', away_name='AWAY'):
    """Post IOC orders concurrently."""
    away_fair_cents = 100 - home_fair_cents
    home_bid = max(1, home_fair_cents - spread_cents)
    away_bid = max(1, away_fair_cents - spread_cents)

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

        if yes_bid > yes_fair:
            yes_bid = yes_fair
        if no_bid > no_fair:
            no_bid = no_fair

        if 1 <= yes_bid <= 99:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_bid,
                'team': f"{yes_team} YES", 'fair': yes_fair,
            })
        if 1 <= no_bid <= 99:
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

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False, args=[
                '--disable-blink-features=AutomationControlled',
            ])
            ctx = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/131.0.0.0 Safari/537.36'),
                viewport={'width': 1280, 'height': 900},
            )
            page = ctx.new_page()
            # Hide webdriver flag
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            full_url = f'{HLTV_BASE}{match_url}'
            print(f"[HLTV] Navigating to {full_url}")

            try:
                page.goto(full_url, timeout=30000, wait_until='domcontentloaded')
                time.sleep(4)
            except Exception as e:
                print(f"[HLTV] Navigation error: {e}")
                browser.close()
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
                # Check if scorebot element exists at all
                sbe = page.query_selector('#scoreboardElement')
                if sbe:
                    print(f"[HLTV] Scorebot element found but no player rows after 30s")
                    print(f"[HLTV] Continuing anyway — data may populate during match")
                else:
                    print("[HLTV] No scorebot element found — stopping")
                    browser.close()
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

                    # Reset bomb lock when round number changes
                    if cur_round != self._prev_round:
                        self._bomb_locked = False
                        self._timer_stall_count = 0
                    self._prev_round = cur_round

                    # Once bomb is detected in a round, lock it on for the rest of that round
                    if self._bomb_locked:
                        state['bomb_planted'] = True
                        state['bomb_source'] = 'locked'
                    elif round_live and timer_s >= 0 and self._prev_timer is not None:
                        prev = self._prev_timer
                        # Timer stall: same value for 2+ polls
                        if timer_s == prev:
                            self._timer_stall_count += 1
                        else:
                            self._timer_stall_count = 0

                        # Big timer drop: round clock (1:55) suddenly becomes C4 fuse (~0:40)
                        # A drop of 30+ seconds in one poll interval = bomb planted
                        timer_jump = prev - timer_s > 30 and timer_s <= 45

                        if self._timer_stall_count >= 2 or timer_jump:
                            state['bomb_planted'] = True
                            state['bomb_source'] = 'timer_jump' if timer_jump else 'timer_stall'
                            self._bomb_locked = True
                    else:
                        self._timer_stall_count = 0

                    # CSS detection from _parse can also lock the bomb
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

            browser.close()

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
    """Scrape HLTV /matches for currently live matches."""
    try:
        from curl_cffi import requests as cffi_requests
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    try:
        session = cffi_requests.Session()
        resp = session.get(f'{HLTV_BASE}/matches', impersonate='chrome131',
                           timeout=30, headers={'Referer': f'{HLTV_BASE}/'})
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
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
        print(f"[HLTV] Live match scrape failed: {e}")
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
        <button class="tab" onclick="switchTab('ou-trade')">O/U Trade</button>
        <button class="tab" onclick="switchTab('predict')">Predict</button>
        <button class="tab" onclick="switchTab('bracket')">Bracket Builder</button>
        <button class="tab" onclick="switchTab('futures')">Futures</button>
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
            Paste a Polymarket event URL or Kalshi event ticker to load all sub-markets.
        </p>

        <div class="controls-row">
            <div class="input-group" style="flex:1;">
                <label>Event URL / Ticker</label>
                <input type="text" id="futures-url" placeholder="https://polymarket.com/event/... or KALSHI:KXCS2..." style="width:100%;">
            </div>
            <button class="btn btn-compute" onclick="futuresFetchEvent()" style="margin-top:18px; padding:10px 20px;">
                Fetch Event
            </button>
            <button class="btn" onclick="futuresImportBracket()" style="margin-top:18px; padding:10px 16px; background:#1a2e1a; border-color:#2e7d32; color:#81c784;">
                Import from Bracket
            </button>
        </div>

        <div id="futures-event-title" style="display:none; margin:10px 0; font-size:14px; color:#4fc3f7; font-weight:600;"></div>

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
                data-ou-tickers='{{ g.ou_tickers_json }}'>
                {{ g.away }} vs {{ g.home }} &mdash; {{ g.home }} {{ g.home_pct }}
            </option>
            {% endfor %}
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
        </div>

        <div class="kbd-help">
            <kbd>C</kbd> Compute &nbsp; <kbd>P</kbd> Auto-Reprice &nbsp;
            <kbd>I</kbd> IOC &nbsp; <kbd>R</kbd> Repost &nbsp; <kbd>X</kbd> Stop+Cancel &nbsp;
            <kbd>J</kbd> Jump &nbsp; <kbd>B</kbd> Buy
            &nbsp; <span id="reprice-status"></span>
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

<script>
let currentGame = null;
let mapResults = [0, 0, 0];
let homeRounds = 0, awayRounds = 0;
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
    };
    mapResults = [0, 0, 0];
    homeRounds = 0;
    awayRounds = 0;
    document.getElementById('home-rounds').textContent = '0';
    document.getElementById('away-rounds').textContent = '0';

    document.getElementById('home-label').textContent = home;
    document.getElementById('away-label').textContent = away;
    document.getElementById('round-home-label').textContent = home;
    document.getElementById('round-away-label').textContent = away;
    document.getElementById('match-section').style.display = 'block';
    document.getElementById('pregame-chip').style.display = 'inline-block';
    document.getElementById('pg-prob').textContent =
        (currentGame.homeProb * 100).toFixed(0) + '%';

    populateTeamDropdowns();
    populateOUDropdowns();

    const hasTickers = currentGame.tickers.length > 0;
    document.getElementById('orderbook-section').style.display = hasTickers ? 'block' : 'none';
    document.getElementById('manual-section').style.display = hasTickers ? 'block' : 'none';
    document.getElementById('jump-section').style.display = hasTickers ? 'block' : 'none';
    document.getElementById('trade-match-content').style.display = 'block';
    if (hasTickers) startOB(); else stopOB();

    const hasOUTickers = currentGame.ouTickers.length > 0;
    document.getElementById('ou-orderbook-section').style.display = hasOUTickers ? 'block' : 'none';
    document.getElementById('ou-manual-section').style.display = hasOUTickers ? 'block' : 'none';
    document.getElementById('ou-jump-section').style.display = hasOUTickers ? 'block' : 'none';
    document.getElementById('ou-match-content').style.display = 'block';
    if (hasOUTickers) ouStartOB(); else ouStopOB();

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
    computeFair();
    ouComputeFair();
}

function updateDisplay() {
    if (!currentGame) return;
    let homeMaps = 0, awayMaps = 0;
    for (let i = 0; i < 3; i++) {
        const slot = document.querySelector('[data-map="'+(i+1)+'"]');
        const result = document.getElementById('map'+(i+1)+'-result');
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
        contracts: parseInt(document.getElementById('contracts').value),
        spread_cents: parseInt(document.getElementById('spread-cents').value),
        tickers: currentGame.tickers,
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
        const tickers = currentGame ? currentGame.tickers : [];
        const resp = await fetch('/api/cancel_all', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers}),
        });
        const data = await resp.json();
        document.getElementById('results').innerHTML =
            '<div class="alert alert-success">' + data.message + '</div>';
    } catch(e) {
        document.getElementById('results').innerHTML =
            '<div class="alert alert-error">Error: ' + e.message + '</div>';
    }
    hideLoading();
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
    if (!currentGame || !currentGame.tickers.length) return;
    try {
        const resp = await fetch('/api/orderbook', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tickers: currentGame.tickers}),
        });
        const data = await resp.json();
        renderOB(data.books);
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
    }
});

// ── Tab switching ─────────────────────────────────────────
function switchTab(tab) {
    activeTab = tab;
    document.getElementById('tab-trade').style.display = tab === 'trade' ? '' : 'none';
    document.getElementById('tab-ou-trade').style.display = tab === 'ou-trade' ? '' : 'none';
    document.getElementById('tab-predict').style.display = tab === 'predict' ? '' : 'none';
    document.getElementById('tab-bracket').style.display = tab === 'bracket' ? '' : 'none';
    document.getElementById('tab-futures').style.display = tab === 'futures' ? '' : 'none';
    const showMatch = (tab === 'trade' || tab === 'ou-trade');
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
}

function runPredict() {
    const a = document.getElementById('pred-team-a').value.trim();
    const b = document.getElementById('pred-team-b').value.trim();
    if (!a || !b) return;
    fetch('/api/predict', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({team_a: a, team_b: b})
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
        if (data.over_under) {
            const ou = data.over_under;
            document.getElementById('pred-ou-row').innerHTML =
                '<div class="ou-chip"><div class="ou-label">Over 2.5</div>' +
                '<div class="ou-pct">' + (ou.over_prob * 100).toFixed(1) + '%</div>' +
                '<div class="ou-fair">' + ou.over_fair + 'c</div></div>' +
                '<div class="ou-chip"><div class="ou-label">Under 2.5</div>' +
                '<div class="ou-pct">' + (ou.under_prob * 100).toFixed(1) + '%</div>' +
                '<div class="ou-fair">' + ou.under_fair + 'c</div></div>';
        }
        document.getElementById('pred-result').style.display = '';
    });
}

// ── Bracket Builder ────────────────────────────────────────────
let BRK_ALL_TEAMS = [];
let brkRounds = [];
let swissMode = false;
let swissStages = [];

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
}

function brkRenameRound(ri) {
    const name = prompt('Round name:', brkRounds[ri].label);
    if (name !== null && name.trim()) { brkRounds[ri].label = name.trim(); brkRender(); }
}

function brkAddGame(ri) {
    brkRounds[ri].games.push({
        slot1: {type:'team', team: BRK_ALL_TEAMS[0]},
        slot2: {type:'team', team: BRK_ALL_TEAMS[0]},
        bo: 3,
    });
    brkRender();
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
            html += ' <select onchange="brkRounds['+ri+'].games['+gi+'].bo=parseInt(this.value)" style="background:#111;color:#4fc3f7;border:1px solid #333;border-radius:3px;font-size:9px;padding:0 2px;">';
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
        swissStages = [{label:'Stage 1', winsToAdv:3, lossesToElim:3, slots:[]}];
        const ct = Math.min(16, T.length);
        for (let i = 0; i < ct; i++) swissStages[0].slots.push({type:'team', team:T[i]});
        document.getElementById('brk-bracket-area').style.display = 'none';
        document.getElementById('swiss-panel').style.display = '';
        swissRender();
        document.getElementById('bracket-results').innerHTML = '';
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
        return;
    }
    swissMode = false;
    swissStages = [];
    document.getElementById('swiss-panel').style.display = 'none';
    document.getElementById('brk-bracket-area').style.display = '';
    brkRender();
    document.getElementById('bracket-results').innerHTML = '';
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
                slots: st.slots.map(s => s.type === 'advance' ? {type:'advance', stage:s.stage} : {type:'team', team:s.team}),
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
    swissStages.push({label:'Stage '+(idx+1), winsToAdv:3, lossesToElim:3, slots:[]});
    const ct = Math.min(16, BRK_ALL_TEAMS.length);
    for (let i = 0; i < ct; i++) swissStages[idx].slots.push({type:'team', team:BRK_ALL_TEAMS[i]});
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
    while (st.slots.length < count) st.slots.push({type:'team', team:BRK_ALL_TEAMS[0]});
    while (st.slots.length > count) st.slots.pop();
    swissRender();
}

function swissSlotChange(si, slotIdx, val) {
    if (val.startsWith('ADV:')) {
        const fromStage = parseInt(val.split(':')[1]);
        swissStages[si].slots[slotIdx] = {type:'advance', stage: fromStage};
    } else {
        swissStages[si].slots[slotIdx] = {type:'team', team: val};
    }
    swissRender();
}

function swissSlotToTeam(si, slotIdx) {
    swissStages[si].slots[slotIdx] = {type:'team', team: BRK_ALL_TEAMS[0]};
    swissRender();
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
        html += '<div><label>Wins to Advance</label><input type="number" min="1" max="5" value="'+st.winsToAdv+'" onchange="swissStages['+si+'].winsToAdv=parseInt(this.value)" style="width:50px;"></div>';
        html += '<div><label>Losses to Elim</label><input type="number" min="1" max="5" value="'+st.lossesToElim+'" onchange="swissStages['+si+'].lossesToElim=parseInt(this.value)" style="width:50px;"></div>';
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
                html += '<select onchange="swissSlotChange('+si+','+i+',this.value)">';
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
            }
            html += '</div>';
        }
        html += '</div>';

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
}

async function swissSimulate() {
    if (swissStages.length === 0) { alert('Add at least one stage'); return; }
    const sims = parseInt(document.getElementById('bracket-sims').value) || 100000;

    const stages = swissStages.map(st => ({
        label: st.label,
        wins_to_advance: st.winsToAdv,
        losses_to_eliminate: st.lossesToElim,
        slots: st.slots.map(s => s.type === 'advance' ? {type:'advance', stage:s.stage} : {type:'team', team:s.team}),
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
    const name = prompt('Name this bracket:');
    if (!name || !name.trim()) return;
    const saves = JSON.parse(localStorage.getItem('cs2_brackets') || '{}');
    saves[name.trim()] = {
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
    try {
        const resp = await fetch('/api/futures/fetch_event', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url}),
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
</script>
</div><!-- /container -->
</body>
</html>
"""


# ── Flask Routes ─────────────────────────────────────────────────────

@app.route('/')
def index():
    tickers = get_kalshi_cs2_tickers()
    games = []
    for key, data in tickers.items():
        home, away = key
        games.append({
            'home': home,
            'away': away,
            'home_prob': data['home_prob'],
            'home_pct': f"{data['home_prob']:.0%}",
            'tickers_json': json.dumps(data['tickers']),
            'ou_tickers_json': json.dumps(data.get('ou_tickers', [])),
        })
    return render_template_string(HTML_TEMPLATE, games=games, dry_run=DRY_RUN,
                                  contracts=CONTRACTS, spread=SPREAD_CENTS)


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

    map_p = series_to_map_prob(home_prob)
    per_map = [map_p] * 3
    map_prob = map_p
    if maps_played < 3 and (home_rounds > 0 or away_rounds > 0):
        map_prob = live_round_win_prob(home_rounds, away_rounds, map_p)
        per_map[maps_played] = map_prob

    # Economy adjustment from scoreboard
    econ_info = {}
    sb_state = HLTV_SCRAPER.get_state() if HLTV_SCRAPER else None
    if sb_state and (sb_state.get('ct_players') or sb_state.get('t_players')):
        ct_players = sb_state.get('ct_players', [])
        t_players = sb_state.get('t_players', [])

        # Figure out which side home team is on
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
            bomb_src = sb_state.get('bomb_source', 'css')
            bomb_debug = sb_state.get('_bomb_debug')
            if bomb_debug and not bomb_planted:
                print(f"[BOMB-DBG] Not planted. Elements: {bomb_debug[:3]}")
            elif bomb_planted:
                print(f"[BOMB] PLANTED ({bomb_src}) | timer={timer_secs}s")
            econ_map_p, econ_detail = economy_adjusted_map_prob(
                ct_players, t_players, map_p, home_is_ct, home_rounds, away_rounds,
                bomb_planted=bomb_planted, timer_seconds=timer_secs)

            if econ_detail:
                # Use economy-adjusted map prob directly
                map_idx = maps_played if maps_played < 3 else 2
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

    live_wp = live_bo3_win_prob(home_maps, away_maps, maps_played, per_map)
    home_fair = max(1, min(99, int(round(live_wp * 100))))
    away_fair = 100 - home_fair

    print(f"[COMPUTE] {away} vs {home} | maps={home_maps}-{away_maps} "
          f"rounds={home_rounds}-{away_rounds} | pregame={home_prob:.3f} "
          f"map={map_prob:.3f} live={live_wp:.3f} -> {home_fair}c")

    alt = compute_alt_lines_bo3(home_maps, away_maps, maps_played, per_map)

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

    if cancel_first:
        ticker_names = [t['ticker'] for t in tickers if 'ticker' in t]
        cancel_all_live_orders(tickers=ticker_names)
        result['cancelled'] = True

    if post_orders_flag and tickers:
        orders = post_live_orders(tickers, home_fair, contracts, spread_cents,
                                  home_name=home, away_name=away)
        result['orders'] = orders
    elif ioc and tickers:
        orders = post_ioc_orders(tickers, home_fair, contracts, spread_cents,
                                 home_name=home, away_name=away)
        result['orders'] = orders

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
    per_map = [map_p] * 3
    if maps_played < 3 and (home_rounds > 0 or away_rounds > 0):
        per_map[maps_played] = live_round_win_prob(home_rounds, away_rounds, map_p)

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
            econ_map_p, econ_detail = economy_adjusted_map_prob(
                sb_state.get('ct_players', []), sb_state.get('t_players', []),
                map_p, home_is_ct, home_rounds, away_rounds,
                bomb_planted=bomb_planted, timer_seconds=timer_secs)
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

        if 1 <= yes_bid <= 99:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': yes_bid,
                'team': 'Over 2.5 YES', 'fair': over_fair_cents,
            })
        if 1 <= no_bid <= 99:
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
    over_bid = max(1, over_fair_cents - spread_cents)
    under_bid = max(1, under_fair_cents - spread_cents)

    if over_bid > over_fair_cents:
        over_bid = over_fair_cents
    if under_bid > under_fair_cents:
        under_bid = under_fair_cents

    orders_to_place = []
    for t in tickers_list:
        ticker = t['ticker']
        if 1 <= over_bid <= 99:
            orders_to_place.append({
                'ticker': ticker, 'side': 'yes', 'price': over_bid,
                'team': 'Over 2.5 YES', 'fair': over_fair_cents,
            })
        if 1 <= under_bid <= 99:
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
    if not team_a or not team_b:
        return jsonify({'error': 'Need team_a and team_b'}), 400
    model, encoders, scale = load_model()
    try:
        prob_a, _, _ = get_win_prob(model, encoders, team_a, team_b, scale)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    from train_model import load_matches, build_training_data
    df = build_training_data(load_matches())
    counts = df['team'].value_counts()
    prob_b = 1 - prob_a
    map_p = series_to_map_prob(prob_a)
    p_2_1 = 2 * map_p * map_p * (1 - map_p)
    p_1_2 = 2 * (1 - map_p) * (1 - map_p) * map_p
    over_prob = p_2_1 + p_1_2
    under_prob = 1 - over_prob
    return jsonify({
        'team_a': team_a, 'team_b': team_b,
        'prob_a': round(prob_a, 4),
        'prob_b': round(prob_b, 4),
        'fair_a': int(round(prob_a * 100)),
        'fair_b': 100 - int(round(prob_a * 100)),
        'ml_a': _prob_to_american(prob_a),
        'ml_b': _prob_to_american(prob_b),
        'games_a': int(counts.get(team_a, 0)),
        'games_b': int(counts.get(team_b, 0)),
        'over_under': {
            'over_prob': round(over_prob, 4),
            'under_prob': round(under_prob, 4),
            'over_fair': int(round(over_prob * 100)),
            'under_fair': int(round(under_prob * 100)),
        },
    })


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
                active = list(range(num_p))
                advanced = []
                eliminated = []

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
                    for key in pools:
                        pool = pools[key]
                        pool.sort(key=lambda pi: seeds[participants[pi]])
                        is_adv_match = wins[pool[0]] == w2a - 1
                        is_elim_match = losses[pool[0]] == l2e - 1
                        wp = wp_by_bo[3] if (is_adv_match or is_elim_match) else wp_by_bo[1]
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

    if 'polymarket.com' in url or (not url.startswith('KALSHI:') and 'kalshi.com' not in url):
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


def _fetch_kalshi_futures(url):
    ticker = url.replace('KALSHI:', '').strip()
    if 'kalshi.com' in ticker:
        parts = ticker.split('/')
        ticker = parts[-1] if parts else ticker

    try:
        params = {'event_ticker': ticker, 'status': 'open', 'limit': 200}
        hdrs = make_auth_headers(PRIVATE_KEY, API_KEY_ID, 'GET',
                                  '/trade-api/v2/markets') if PRIVATE_KEY else {}
        resp = http_requests.get(f"{KALSHI_BASE}/markets", headers=hdrs,
                                  params=params, timeout=10)
        raw_markets = []
        if resp.status_code == 200:
            raw_markets = resp.json().get('markets', [])
        if not raw_markets:
            params = {'series_ticker': ticker, 'status': 'open', 'limit': 200}
            resp = http_requests.get(f"{KALSHI_BASE}/markets", headers=hdrs,
                                      params=params, timeout=10)
            if resp.status_code == 200:
                raw_markets = resp.json().get('markets', [])
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

            # Yes side
            bid_yes = round(fair - spread_cents / 100, 2)
            ob_yes = get_poly_orderbook(token_yes)
            ask_yes = poly_best_ask(ob_yes)
            if bid_yes >= ask_yes:
                bid_yes = round(ask_yes - tick, 2)

            if bid_yes >= tick:
                if DRY_RUN or not POLY_CLIENT:
                    posted.append(f'YES@{bid_yes:.2f}(dry)')
                else:
                    POLY_CLIENT.cancel_token_orders(token_yes)
                    resp = POLY_CLIENT.place_order(token_yes, "BUY", bid_yes, size,
                                                    tick_size=tick_size, neg_risk=neg_risk)
                    posted.append(f'YES@{bid_yes:.2f}' if resp else 'YES:FAIL')
                with FUTURES_TOKEN_LOCK:
                    FUTURES_TOKEN_IDS.append(token_yes)

            # No side
            if token_no:
                fair_no = 1.0 - fair
                bid_no = round(fair_no - spread_cents / 100, 2)
                ob_no = get_poly_orderbook(token_no)
                ask_no = poly_best_ask(ob_no)
                if bid_no >= ask_no:
                    bid_no = round(ask_no - tick, 2)

                if bid_no >= tick:
                    if DRY_RUN or not POLY_CLIENT:
                        posted.append(f'NO@{bid_no:.2f}(dry)')
                    else:
                        POLY_CLIENT.cancel_token_orders(token_no)
                        resp = POLY_CLIENT.place_order(token_no, "BUY", bid_no, size,
                                                        tick_size=tick_size, neg_risk=neg_risk)
                        posted.append(f'NO@{bid_no:.2f}' if resp else 'NO:FAIL')
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

            # Yes side
            bid_yes = max(1, round(fair * 100) - spread_cents)
            if ob and ob.get('yes_best_ask') is not None:
                if bid_yes >= ob['yes_best_ask']:
                    bid_yes = ob['yes_best_ask'] - 1

            if bid_yes >= 1:
                if DRY_RUN:
                    posted.append(f'YES@{bid_yes}c(dry)')
                else:
                    resp = place_limit_order(API_KEY_ID, PRIVATE_KEY, ticker, 'yes',
                                              size, bid_yes, DRY_RUN)
                    posted.append(f'YES@{bid_yes}c' if resp else 'YES:FAIL')

            # No side
            fair_no = 1.0 - fair
            bid_no = max(1, round(fair_no * 100) - spread_cents)
            if ob and ob.get('no_best_ask') is not None:
                if bid_no >= ob['no_best_ask']:
                    bid_no = ob['no_best_ask'] - 1

            if bid_no >= 1:
                if DRY_RUN:
                    posted.append(f'NO@{bid_no}c(dry)')
                else:
                    resp = place_limit_order(API_KEY_ID, PRIVATE_KEY, ticker, 'no',
                                              size, bid_no, DRY_RUN)
                    posted.append(f'NO@{bid_no}c' if resp else 'NO:FAIL')

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

    wp_bo1 = np.copy(map_wp)
    wp_bo3 = np.full((n, n), 0.5)
    for i in range(n):
        for j in range(i + 1, n):
            wp_bo3[i][j] = map_prob_to_series(map_wp[i][j], 3)
            wp_bo3[j][i] = 1.0 - wp_bo3[i][j]

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
            active = list(range(num_p))
            advanced = []
            eliminated = []

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

                for key in pools:
                    pool = pools[key]
                    pool.sort(key=lambda pi: seeds[participants[pi]])

                    is_adv_match = wins[pool[0]] == w2a - 1
                    is_elim_match = losses[pool[0]] == l2e - 1
                    wp = wp_bo3 if (is_adv_match or is_elim_match) else wp_bo1

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
                        leftover = pool[half]
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


# ── Main ─────────────────────────────────────────────────────────────

def main():
    global API_KEY_ID, PRIVATE_KEY, DRY_RUN, CONTRACTS, SPREAD_CENTS, POLY_CLIENT

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
    else:
        API_KEY_ID = 'dry-run'

    if args.poly_key_path:
        POLY_CLIENT = PolyClient(args.poly_key_path)
        POLY_CLIENT.start_heartbeat()

    global HLTV_SCRAPER
    HLTV_SCRAPER = HLTVScoreboard()

    print(f"\n  CS2 Live Trader")
    print(f"  Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"  Contracts: {CONTRACTS} | Spread: {SPREAD_CENTS}c")
    print(f"  Polymarket: {'connected' if POLY_CLIENT else 'off'}")
    print(f"  http://localhost:{args.port}\n")

    app.run(host='0.0.0.0', port=args.port, debug=False)


if __name__ == '__main__':
    main()