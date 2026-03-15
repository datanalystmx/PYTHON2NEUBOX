#!/usr/bin/env python3
"""
tg_alertas.py — Envía proyecciones NBA a Telegram
Lee los JSONs directamente desde Neubox vía HTTP — sin importar NBA.py
Scheduled Task en PythonAnywhere: python3 /home/datanalyst/NBA/tg_alertas.py
"""

import urllib.request, urllib.parse, json
from datetime import datetime

# ── Configuración ─────────────────────────────────────────────
TOKEN   = '8056519072:AAEvCrRXWz1udyFPfHLKBhaHKkMUuBZQDsY'
CHAT_ID = '8512414329'

# URL base de los JSONs en Neubox
BASE_URL = 'https://nba.nexus-core.com.mx/data/'

def fetch_json(filename):
    """Descarga un JSON desde Neubox."""
    try:
        url = BASE_URL + filename
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'[ERROR] No se pudo cargar {filename}: {e}')
        return None

def tg_send(text: str) -> bool:
    url  = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    data = urllib.parse.urlencode({
        'chat_id':    CHAT_ID,
        'text':       text,
        'parse_mode': 'HTML',
    }).encode()
    req = urllib.request.Request(url, data=data, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read())
            return resp.get('ok', False)
    except Exception as e:
        print(f'[TG] Error al enviar: {e}')
        return False

def tg_escape(s):
    return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def fmt_hora_cdmx(time_ct):
    if not time_ct:
        return '—'
    try:
        import re
        # Formato '06:30 PM CT' → convertir a CDMX (+1h)
        m = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM)\s*CT', str(time_ct), re.IGNORECASE)
        if m:
            h  = int(m.group(1))
            mn = m.group(2)
            ap = m.group(3).upper()
            # Convertir a 24h
            if ap == 'PM' and h != 12: h += 12
            if ap == 'AM' and h == 12: h = 0
            h += 1  # CT → CDMX
            if h >= 24: h -= 24
            ap2 = 'PM' if h >= 12 else 'AM'
            h12 = h - 12 if h > 12 else (12 if h == 0 else h)
            return f'{h12}:{mn} {ap2}'
        # Formato ISO UTC
        m2 = re.match(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})', str(time_ct))
        if m2:
            h  = int(m2.group(4)) + 1
            mn = m2.group(5)
            ap = 'PM' if h >= 12 else 'AM'
            h12 = h - 12 if h > 12 else (12 if h == 0 else h)
            return f'{h12}:{mn} {ap}'
    except:
        pass
    return str(time_ct)

def build_message(games: list) -> str:
    hoy   = datetime.now().strftime('%A %d %b %Y').upper()
    lines = []
    lines.append(f'🏀 <b>NBA · {hoy}</b>')
    lines.append(f'<code>Proyecciones Modelo v9.3 — {len(games)} juegos</code>')
    lines.append('')

    for i, g in enumerate(games, 1):
        away      = tg_escape(g.get('away', ''))
        home      = tg_escape(g.get('home', ''))
        away_abbr = tg_escape(g.get('away_abbr', ''))
        home_abbr = tg_escape(g.get('home_abbr', ''))
        hora      = fmt_hora_cdmx(g.get('time_ct', ''))

        a_road = g.get('away_road', {});  h_home = g.get('home_home', {})
        a_rec  = f"{a_road.get('W',0)}-{a_road.get('L',0)} road"
        h_rec  = f"{h_home.get('W',0)}-{h_home.get('L',0)} home"

        pts_a  = g.get('pts_away', 0)
        pts_h  = g.get('pts_home', 0)
        total  = g.get('total', 0)
        spread = g.get('spread', 0)
        p_home = round(g.get('p_home', 0.5) * 100)
        p_away = 100 - p_home
        ht_a   = g.get('ht_away', 0)
        ht_h   = g.get('ht_home', 0)
        ht_tot = g.get('ht_total', 0)

        if spread > 0:
            fav = home_abbr; fav_pct = p_home; dog = away_abbr
        else:
            fav = away_abbr; fav_pct = p_away; dog = home_abbr

        spread_str = (f'{home_abbr} -{abs(spread):.1f}' if spread > 0
                      else f'{away_abbr} -{abs(spread):.1f}')

        alerts = []
        for a in g.get('alerts', []):
            label = a.get('label', '')
            if   label == 'BLOWOUT RISK': alerts.append('💥 BLOWOUT RISK')
            elif label == 'TANK':         alerts.append('📉 TANK')
            elif label == 'B2B':          alerts.append('🔄 B2B')
            elif 'OUT' in label:          alerts.append(f'🚑 {tg_escape(label)}')

        alert_str = '  ' + ' · '.join(alerts) if alerts else ''

        lines.append(f'┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄')
        lines.append(f'<b>JUEGO {i} · {hora} CDMX</b>{alert_str}')
        lines.append(f'✈️ <b>{away}</b>  <code>({a_rec})</code>')
        lines.append(f'🏠 <b>{home}</b>  <code>({h_rec})</code>')
        lines.append('')
        lines.append(f'🏆 Ganador: <b>{fav} {fav_pct}%</b>  vs  {dog} {100-fav_pct}%')
        lines.append(f'📈 Spread: <code>{spread_str}</code>')
        lines.append(f'🎯 Total O/U: <b>{total:.1f} pts</b>')
        lines.append(f'   ├ {away_abbr}: <code>{pts_a:.1f}</code>  🏠 {home_abbr}: <code>{pts_h:.1f}</code>')
        lines.append(f'   └ 1ª Mitad: <b>{ht_tot:.1f} pts</b>  ({away_abbr} {ht_a:.1f} · {home_abbr} {ht_h:.1f})')
        lines.append('')

    lines.append('┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄')
    lines.append('<i>NBA War Room · Modelo Matemático v9.3</i>')
    return '\n'.join(lines)

def main():
    print(f'[TG] Cargando proyecciones desde {BASE_URL}projections.json...')

    data = fetch_json('projections.json')
    if not data:
        print('[ERROR] No se pudo cargar projections.json')
        return

    # projections.json puede ser lista directa o dict con key 'games'
    if isinstance(data, list):
        games = data
    elif isinstance(data, dict):
        games = data.get('games', [])
    else:
        games = []

    if not games:
        print('[TG] No hay proyecciones hoy — mensaje no enviado.')
        return

    print(f'[TG] {len(games)} juegos encontrados. Construyendo mensaje…')
    msg = build_message(games)

    ok = tg_send(msg)
    if ok:
        print(f'✅ Mensaje enviado a Telegram! ({len(games)} juegos)')
    else:
        print('❌ Error al enviar a Telegram.')

if __name__ == '__main__':
    main()
