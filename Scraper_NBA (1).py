"""
NBA STATS COMPLETO - 54 hojas → JSONs + FTP a Neubox
v9: Guarda JSONs en vez de Excel y los sube automáticamente a Neubox
"""
import time
import json
import os
import ftplib
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from nba_api.stats.endpoints import (
    leaguestandingsv3,
    leaguedashteamstats,
    leaguedashplayerstats,
    leaguedashplayerbiostats,
    commonteamroster,
    scoreboardv3,
)
from nba_api.stats.static import teams

SEASON      = '2025-26'
SEASON_TYPE = 'Regular Season'
LOCAL_DIR   = './data/'   # carpeta local temporal en GitHub Actions
LAST_GAMES  = './last_games.json'

# ── FTP Neubox ────────────────────────────────────────────────
FTP_HOST = 'ftp.nexus-core.com.mx'
FTP_USER = 'servicios@nexus-core.com.mx'
FTP_PASS = os.environ.get('FTP_PASS', 'S3rvic3$1984*$*')
FTP_DIR  = '/public_html/data/'   # carpeta destino en Neubox

# ── Proxies Webshare ──────────────────────────────────────────
PROXIES_LIST = [
    'http://pblneydf:j53331earxr3@31.59.20.176:6754',
    'http://pblneydf:j53331earxr3@23.95.150.145:6114',
    'http://pblneydf:j53331earxr3@198.23.239.134:6540',
    'http://pblneydf:j53331earxr3@45.38.107.97:6014',
    'http://pblneydf:j53331earxr3@107.172.163.27:6543',
    'http://pblneydf:j53331earxr3@198.105.121.200:6462',
    'http://pblneydf:j53331earxr3@64.137.96.74:6641',
    'http://pblneydf:j53331earxr3@216.10.27.159:6837',
    'http://pblneydf:j53331earxr3@142.111.67.146:5611',
    'http://pblneydf:j53331earxr3@23.26.53.37:6003',
]

ABBR_MAP = {
    'atlanta hawks':'ATL','boston celtics':'BOS','brooklyn nets':'BKN',
    'charlotte hornets':'CHA','chicago bulls':'CHI','cleveland cavaliers':'CLE',
    'dallas mavericks':'DAL','denver nuggets':'DEN','detroit pistons':'DET',
    'golden state warriors':'GSW','houston rockets':'HOU','indiana pacers':'IND',
    'los angeles clippers':'LAC','la clippers':'LAC','l.a. clippers':'LAC',
    'los angeles lakers':'LAL','la lakers':'LAL','l.a. lakers':'LAL',
    'memphis grizzlies':'MEM','miami heat':'MIA','milwaukee bucks':'MIL',
    'minnesota timberwolves':'MIN','new orleans pelicans':'NOP','new york knicks':'NYK',
    'oklahoma city thunder':'OKC','orlando magic':'ORL','philadelphia 76ers':'PHI',
    'phoenix suns':'PHX','portland trail blazers':'POR','sacramento kings':'SAC',
    'san antonio spurs':'SAS','toronto raptors':'TOR','utah jazz':'UTA',
    'washington wizards':'WAS',
}

def to_abbr(team_name):
    k = str(team_name).lower().strip()
    return ABBR_MAP.get(k, ''.join(w[0] for w in k.split()).upper()[:3])

os.makedirs(LOCAL_DIR, exist_ok=True)

# ── PROXY ─────────────────────────────────────────────────────
def get_working_proxy():
    print("Buscando proxy funcional...")
    for proxy in PROXIES_LIST:
        try:
            r = requests.get(
                'https://ipv4.webshare.io/',
                proxies={'http': proxy, 'https': proxy},
                timeout=10
            )
            if r.status_code < 500:
                print(f"  Proxy OK: {proxy.split('@')[1]}")
                return proxy
        except:
            print(f"  Fallo: {proxy.split('@')[1]}")
    print("  Ningún proxy funcionó, intentando sin proxy...")
    return None

PROXY = get_working_proxy()

# ── FTP: subir un archivo a Neubox ────────────────────────────
def ftp_upload(local_path, remote_filename):
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.set_pasv(True)
        ftp.cwd(FTP_DIR)
        with open(local_path, 'rb') as f:
            ftp.storbinary(f'STOR {remote_filename}', f)
        ftp.quit()
        print(f"  ✅ FTP OK: {remote_filename}")
    except Exception as e:
        print(f"  ❌ FTP ERROR {remote_filename}: {e}")

# ── Guardar JSON local + subir por FTP ────────────────────────
def guardar_json(nombre, datos):
    """Guarda lista de dicts como JSON y sube a Neubox."""
    filename = nombre + '.json'
    local_path = LOCAL_DIR + filename
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(datos, f, ensure_ascii=False, indent=2, default=str)
    print(f"  💾 {filename}: {len(datos)} filas")
    ftp_upload(local_path, filename)

# ── all_sheets: almacena DataFrames igual que antes ──────────
all_sheets = {}

def guardar_hoja(nombre, df):
    if df is not None and len(df) > 0:
        all_sheets[nombre] = df
        print(f"  OK {nombre}: {len(df)} filas, {len(df.columns)} columnas")
    else:
        print(f"  SIN DATOS {nombre}")

def pausa():
    time.sleep(1)

# ═══════════════════════════════════════════════════════════════
#  B2B — juegos de ayer → last_games.json
# ═══════════════════════════════════════════════════════════════
print("\nB2B — Guardando juegos de AYER en last_games.json...")
try:
    et_tz_b2b = pytz.timezone('America/New_York')
    ayer_et   = datetime.now(et_tz_b2b) - timedelta(days=1)
    ayer_str  = ayer_et.strftime('%m/%d/%Y')
    ayer_iso  = ayer_et.strftime('%Y-%m-%d')

    print(f"  Consultando API para fecha: {ayer_str}")
    sb_ayer = scoreboardv3.ScoreboardV3(
        game_date=ayer_str, league_id='00',
        proxy=PROXY, timeout=60
    )
    time.sleep(1)
    data_ayer  = sb_ayer.get_dict()
    games_ayer = data_ayer['scoreboard']['games']

    b2b_teams = []
    for game in games_ayer:
        away = game['awayTeam']['teamCity'] + ' ' + game['awayTeam']['teamName']
        home = game['homeTeam']['teamCity'] + ' ' + game['homeTeam']['teamName']
        b2b_teams.append(to_abbr(away))
        b2b_teams.append(to_abbr(home))
    b2b_teams = list(set(t for t in b2b_teams if t))

    last_games_data = {
        'date':       ayer_iso,
        'teams':      b2b_teams,
        'updated_at': datetime.now().isoformat(),
    }
    # Guardar local
    with open(LAST_GAMES, 'w', encoding='utf-8') as f:
        json.dump(last_games_data, f, ensure_ascii=False, indent=2)
    # Subir a Neubox
    ftp_upload(LAST_GAMES, 'last_games.json')

    print(f"  last_games.json — {len(b2b_teams)} equipos: {b2b_teams}")
except Exception as e:
    print(f"  Error B2B: {e}")

# ─── STANDINGS ────────────────────────────────────────────────
print("\nSTANDINGS...")
try:
    st = leaguestandingsv3.LeagueStandingsV3(
        season=SEASON, season_type=SEASON_TYPE,
        proxy=PROXY, timeout=60
    )
    pausa()
    guardar_hoja("Standings", st.get_data_frames()[0])
except Exception as e:
    print(f"  ERROR Standings: {e}")

# ─── STATS DE EQUIPOS BASE ────────────────────────────────────
def get_team_stats(nombre, location=''):
    print(f"\n{nombre}...")
    try:
        params = dict(
            season=SEASON, season_type_all_star=SEASON_TYPE,
            per_mode_detailed='PerGame', proxy=PROXY, timeout=60
        )
        if location:
            params['location_nullable'] = location
        ep = leaguedashteamstats.LeagueDashTeamStats(**params)
        pausa()
        guardar_hoja(nombre, ep.get_data_frames()[0])
    except Exception as e:
        print(f"  ERROR {nombre}: {e}")

def get_opponent_stats(nombre, location=''):
    print(f"\n{nombre}...")
    try:
        params = dict(
            season=SEASON, season_type_all_star=SEASON_TYPE,
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Opponent',
            proxy=PROXY, timeout=60
        )
        if location:
            params['location_nullable'] = location
        ep = leaguedashteamstats.LeagueDashTeamStats(**params)
        pausa()
        guardar_hoja(nombre, ep.get_data_frames()[0])
    except Exception as e:
        print(f"  ERROR {nombre}: {e}")

def get_advanced_stats(nombre, location=''):
    print(f"\n{nombre}...")
    try:
        params = dict(
            season=SEASON, season_type_all_star=SEASON_TYPE,
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Advanced',
            proxy=PROXY, timeout=60
        )
        if location:
            params['location_nullable'] = location
        ep = leaguedashteamstats.LeagueDashTeamStats(**params)
        pausa()
        guardar_hoja(nombre, ep.get_data_frames()[0])
    except Exception as e:
        print(f"  ERROR {nombre}: {e}")

get_team_stats("Total_Stats")
get_team_stats("Home_Stats",  location='Home')
get_team_stats("Road_Stats",  location='Road')
get_opponent_stats("Total_Opponent")
get_opponent_stats("Opponent_Home", location='Home')
get_opponent_stats("Opponent_Road", location='Road')
get_advanced_stats("Total_Teams_Advanced")
get_advanced_stats("Teams_Advanced_Home", location='Home')
get_advanced_stats("Teams_Advanced_Road", location='Road')

# ─── STATS POR PERIODO Y MITAD ────────────────────────────────
def get_period_stats(nombre, measure_type, period, location='', game_segment=''):
    print(f"\n{nombre}...")
    try:
        params = dict(
            season=SEASON,
            season_type_all_star=SEASON_TYPE,
            per_mode_detailed='PerGame',
            measure_type_detailed_defense=measure_type,
            period=period,
            proxy=PROXY,
            timeout=60
        )
        if location:
            params['location_nullable'] = location
        if game_segment:
            params['game_segment_nullable'] = game_segment
        ep = leaguedashteamstats.LeagueDashTeamStats(**params)
        pausa()
        guardar_hoja(nombre, ep.get_data_frames()[0])
    except Exception as e:
        print(f"  ERROR {nombre}: {e}")

print("\n=== TRADITIONAL POR PERIODO/MITAD ===")
get_period_stats("Trad_Q1_Total", "Base", period=1)
get_period_stats("Trad_Q2_Total", "Base", period=2)
get_period_stats("Trad_Q3_Total", "Base", period=3)
get_period_stats("Trad_Q4_Total", "Base", period=4)
get_period_stats("Trad_1H_Total", "Base", period=0, game_segment="First Half")
get_period_stats("Trad_2H_Total", "Base", period=0, game_segment="Second Half")
get_period_stats("Trad_Q1_Home",  "Base", period=1, location="Home")
get_period_stats("Trad_Q2_Home",  "Base", period=2, location="Home")
get_period_stats("Trad_Q3_Home",  "Base", period=3, location="Home")
get_period_stats("Trad_Q4_Home",  "Base", period=4, location="Home")
get_period_stats("Trad_1H_Home",  "Base", period=0, location="Home", game_segment="First Half")
get_period_stats("Trad_2H_Home",  "Base", period=0, location="Home", game_segment="Second Half")
get_period_stats("Trad_Q1_Road",  "Base", period=1, location="Road")
get_period_stats("Trad_Q2_Road",  "Base", period=2, location="Road")
get_period_stats("Trad_Q3_Road",  "Base", period=3, location="Road")
get_period_stats("Trad_Q4_Road",  "Base", period=4, location="Road")
get_period_stats("Trad_1H_Road",  "Base", period=0, location="Road", game_segment="First Half")
get_period_stats("Trad_2H_Road",  "Base", period=0, location="Road", game_segment="Second Half")

print("\n=== OPPONENT POR PERIODO/MITAD ===")
get_period_stats("Opp_Q1_Total",  "Opponent", period=1)
get_period_stats("Opp_Q2_Total",  "Opponent", period=2)
get_period_stats("Opp_Q3_Total",  "Opponent", period=3)
get_period_stats("Opp_Q4_Total",  "Opponent", period=4)
get_period_stats("Opp_1H_Total",  "Opponent", period=0, game_segment="First Half")
get_period_stats("Opp_2H_Total",  "Opponent", period=0, game_segment="Second Half")
get_period_stats("Opp_Q1_Home",   "Opponent", period=1, location="Home")
get_period_stats("Opp_Q2_Home",   "Opponent", period=2, location="Home")
get_period_stats("Opp_Q3_Home",   "Opponent", period=3, location="Home")
get_period_stats("Opp_Q4_Home",   "Opponent", period=4, location="Home")
get_period_stats("Opp_1H_Home",   "Opponent", period=0, location="Home", game_segment="First Half")
get_period_stats("Opp_2H_Home",   "Opponent", period=0, location="Home", game_segment="Second Half")
get_period_stats("Opp_Q1_Road",   "Opponent", period=1, location="Road")
get_period_stats("Opp_Q2_Road",   "Opponent", period=2, location="Road")
get_period_stats("Opp_Q3_Road",   "Opponent", period=3, location="Road")
get_period_stats("Opp_Q4_Road",   "Opponent", period=4, location="Road")
get_period_stats("Opp_1H_Road",   "Opponent", period=0, location="Road", game_segment="First Half")
get_period_stats("Opp_2H_Road",   "Opponent", period=0, location="Road", game_segment="Second Half")

# ─── STATS DE JUGADORES ───────────────────────────────────────
def get_player_stats(nombre, location=''):
    print(f"\n{nombre}...")
    try:
        params = dict(
            season=SEASON, season_type_all_star=SEASON_TYPE,
            per_mode_detailed='PerGame', proxy=PROXY, timeout=60
        )
        if location:
            params['location_nullable'] = location
        ep = leaguedashplayerstats.LeagueDashPlayerStats(**params)
        pausa()
        guardar_hoja(nombre, ep.get_data_frames()[0])
    except Exception as e:
        print(f"  ERROR {nombre}: {e}")

def get_player_misc(nombre, location=''):
    print(f"\n{nombre}...")
    try:
        params = dict(
            season=SEASON,
            season_type_all_star=SEASON_TYPE,
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Misc',
            proxy=PROXY,
            timeout=60
        )
        if location:
            params['location_nullable'] = location
        ep = leaguedashplayerstats.LeagueDashPlayerStats(**params)
        pausa()
        guardar_hoja(nombre, ep.get_data_frames()[0])
    except Exception as e:
        print(f"  ERROR {nombre}: {e}")

get_player_stats("Total_Player_Stats")
get_player_stats("Players_Home", location='Home')
get_player_stats("Players_Road", location='Road')
get_player_misc("Players_Misc_Total")
get_player_misc("Players_Misc_Home", location='Home')
get_player_misc("Players_Misc_Road", location='Road')

# ─── PLAYER BIO + POSICIÓN ───────────────────────────────────
print(f"\nPlayer_Positions...")
try:
    ep = leaguedashplayerbiostats.LeagueDashPlayerBioStats(
        season=SEASON, season_type_all_star=SEASON_TYPE,
        per_mode_simple='PerGame', proxy=PROXY, timeout=60
    )
    pausa()
    df_bio = ep.get_data_frames()[0]

    print(f"  Obteniendo posiciones desde rosters...")
    all_teams_list = teams.get_teams()
    roster_rows    = []
    for team in all_teams_list:
        try:
            r    = commonteamroster.CommonTeamRoster(
                team_id=team['id'], season=SEASON,
                proxy=PROXY, timeout=60
            )
            df_r = r.get_data_frames()[0]
            roster_rows.append(df_r[['PLAYER_ID', 'POSITION']])
            time.sleep(0.6)
        except Exception as ex:
            print(f"    {team['abbreviation']}: {ex}")

    if roster_rows:
        df_positions = pd.concat(roster_rows, ignore_index=True).drop_duplicates('PLAYER_ID')
        df_bio       = df_bio.merge(df_positions, on='PLAYER_ID', how='left')
        print(f"  Posiciones: {df_bio['POSITION'].notna().sum()} jugadores")
    else:
        print(f"  No se pudieron obtener posiciones")

    guardar_hoja("Player_Positions", df_bio)
except Exception as e:
    import traceback
    print(f"  ERROR Player_Positions: {e}")
    traceback.print_exc()

# ─── DAILY SCHEDULE ──────────────────────────────────────────
print(f"\nDaily_Schedule...")

def get_juegos_para_fecha(fecha_dt, proxy, ct_tz):
    fecha_str = fecha_dt.strftime('%m/%d/%Y')
    try:
        sb   = scoreboardv3.ScoreboardV3(
            game_date=fecha_str, league_id='00',
            proxy=proxy, timeout=60
        )
        pausa()
        data   = sb.get_dict()
        juegos = []
        for game in data['scoreboard']['games']:
            game_time_utc = game.get('gameTimeUTC', '')
            try:
                gt      = datetime.strptime(game_time_utc, '%Y-%m-%dT%H:%M:%SZ')
                gt_utc  = pytz.utc.localize(gt)
                hora_ct = gt_utc.astimezone(ct_tz).strftime('%I:%M %p CT')
            except:
                hora_ct = game.get('gameStatusText', '')

            away = game['awayTeam']
            home = game['homeTeam']
            juegos.append({
                'GAME_ID':    game['gameId'],
                'DATE':       fecha_str,
                'TIME_CT':    hora_ct,
                'AWAY_TEAM':  away['teamCity'] + ' ' + away['teamName'],
                'HOME_TEAM':  home['teamCity'] + ' ' + home['teamName'],
                'AWAY_SCORE': away.get('score', ''),
                'HOME_SCORE': home.get('score', ''),
                'STATUS':     game.get('gameStatusText', ''),
            })
        return fecha_str, juegos
    except Exception as e:
        print(f"    Error obteniendo {fecha_str}: {e}")
        return fecha_str, []

try:
    et_tz  = pytz.timezone('America/New_York')
    ct_tz  = pytz.timezone('America/Chicago')
    hoy_et = datetime.now(et_tz)

    print(f"  Hora ET: {hoy_et.strftime('%Y-%m-%d %H:%M')} → buscando juegos de hoy")
    fecha_str, juegos = get_juegos_para_fecha(hoy_et, PROXY, ct_tz)
    print(f"  Fecha: {fecha_str} → {len(juegos)} juego(s)")

    if len(juegos) == 0:
        print(f"  Sin juegos hoy — probando MANANA...")
        fecha_str2, juegos2 = get_juegos_para_fecha(hoy_et + timedelta(days=1), PROXY, ct_tz)
        if len(juegos2) > 0:
            fecha_str, juegos = fecha_str2, juegos2
            print(f"  Fallback MANANA ({fecha_str}): {len(juegos)} juego(s)")
        else:
            print(f"  Sin juegos manana — probando AYER...")
            fecha_str3, juegos3 = get_juegos_para_fecha(hoy_et - timedelta(days=1), PROXY, ct_tz)
            if len(juegos3) > 0:
                fecha_str, juegos = fecha_str3, juegos3
                print(f"  Fallback AYER ({fecha_str}): {len(juegos)} juego(s)")
            else:
                print(f"  Sin juegos en ninguna fecha")

    guardar_hoja("Daily_Schedule", pd.DataFrame(juegos))
    print(f"  {len(juegos)} juegos para {fecha_str}")

except Exception as e:
    import traceback
    print(f"  ERROR Daily_Schedule: {e}")
    traceback.print_exc()

# ═══════════════════════════════════════════════════════════════
# GUARDAR JSONs Y SUBIR POR FTP
# ═══════════════════════════════════════════════════════════════
print(f"\nGuardando {len(all_sheets)} hojas como JSON y subiendo a Neubox...")

# Mapeo de hojas → estructura JSON para la web PHP
# La web PHP necesita estos JSONs específicos:

# 1. standings.json — lista plana
if 'Standings' in all_sheets:
    datos = all_sheets['Standings'].to_dict(orient='records')
    guardar_json('standings', datos)

# 2. team_stats_all.json — {total: {}, home: {}, road: {}}
team_stats_all = {}
for key, sheet in [('total','Total_Stats'), ('home','Home_Stats'), ('road','Road_Stats')]:
    if sheet in all_sheets:
        df = all_sheets[sheet]
        team_stats_all[key] = {
            row['TEAM_NAME']: row
            for row in df.to_dict(orient='records')
            if row.get('TEAM_NAME')
        }
guardar_json('team_stats_all', team_stats_all)

# 3. opp_stats_all.json — {total: {}, home: {}, road: {}}
opp_stats_all = {}
for key, sheet in [('total','Total_Opponent'), ('home','Opponent_Home'), ('road','Opponent_Road')]:
    if sheet in all_sheets:
        df = all_sheets[sheet]
        opp_stats_all[key] = {
            row['TEAM_NAME']: row
            for row in df.to_dict(orient='records')
            if row.get('TEAM_NAME')
        }
guardar_json('opp_stats_all', opp_stats_all)

# 4. advanced_stats.json — {total: {}, home: {}, road: {}}
adv_stats_all = {}
for key, sheet in [('total','Total_Teams_Advanced'), ('home','Teams_Advanced_Home'), ('road','Teams_Advanced_Road')]:
    if sheet in all_sheets:
        df = all_sheets[sheet]
        adv_stats_all[key] = {
            row['TEAM_NAME']: row
            for row in df.to_dict(orient='records')
            if row.get('TEAM_NAME')
        }
guardar_json('advanced_stats', adv_stats_all)

# 5. quarter_stats.json — {Trad_Q1_Total: {team: {}}, ...}
quarter_sheets = [
    'Trad_Q1_Total','Trad_Q2_Total','Trad_Q3_Total','Trad_Q4_Total',
    'Trad_1H_Total','Trad_2H_Total',
    'Trad_Q1_Home','Trad_Q2_Home','Trad_Q3_Home','Trad_Q4_Home',
    'Trad_1H_Home','Trad_2H_Home',
    'Trad_Q1_Road','Trad_Q2_Road','Trad_Q3_Road','Trad_Q4_Road',
    'Trad_1H_Road','Trad_2H_Road',
    'Opp_Q1_Total','Opp_Q2_Total','Opp_Q3_Total','Opp_Q4_Total',
    'Opp_1H_Total','Opp_2H_Total',
    'Opp_Q1_Home','Opp_Q2_Home','Opp_Q3_Home','Opp_Q4_Home',
    'Opp_1H_Home','Opp_2H_Home',
    'Opp_Q1_Road','Opp_Q2_Road','Opp_Q3_Road','Opp_Q4_Road',
    'Opp_1H_Road','Opp_2H_Road',
]
quarter_stats = {}
for sheet in quarter_sheets:
    if sheet in all_sheets:
        df = all_sheets[sheet]
        quarter_stats[sheet] = {
            row['TEAM_NAME']: row
            for row in df.to_dict(orient='records')
            if row.get('TEAM_NAME')
        }
guardar_json('quarter_stats', quarter_stats)

# 6. player_stats.json — {Total_Player_Stats: [], Players_Home: [], ...}
player_sheets = [
    'Total_Player_Stats', 'Players_Home', 'Players_Road',
    'Players_Misc_Total', 'Players_Misc_Home', 'Players_Misc_Road',
    'Player_Positions',
]
player_stats = {}
for sheet in player_sheets:
    if sheet in all_sheets:
        player_stats[sheet] = all_sheets[sheet].to_dict(orient='records')
guardar_json('player_stats', player_stats)

# 7. daily_schedule.json — {games: [], date: ''}
if 'Daily_Schedule' in all_sheets:
    juegos_lista = all_sheets['Daily_Schedule'].to_dict(orient='records')
    schedule_json = {'games': juegos_lista, 'date': fecha_str}
    filename = 'daily_schedule.json'
    local_path = LOCAL_DIR + filename
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(schedule_json, f, ensure_ascii=False, indent=2, default=str)
    print(f"  💾 {filename}: {len(juegos_lista)} juegos")
    ftp_upload(local_path, filename)

print(f"\n✅ LISTO — {len(all_sheets)} hojas procesadas")
print(f"JSONs subidos a Neubox: {FTP_HOST}{FTP_DIR}")
