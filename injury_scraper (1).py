#!/usr/bin/env python3
# =============================================================================
#  NBA INJURY SCRAPER 2026 · v10.5
#
#  FIXES vs v10.4:
#  ✅ normalize_player_name(): sufijo pegado al apellido sin espacio
#     "ButlerIII" → "BUTLER III", "LivelyII" → "LIVELY II"
#     El regex ahora acepta sufijo con O sin espacio previo.
# =============================================================================

import requests, json, os, re, io, time, ftplib
from datetime import datetime, timezone, timedelta

try:
    import pdfplumber
except ImportError:
    print("ERROR: pip install pdfplumber --break-system-packages")
    exit(1)

OUTPUT_FILE = './injury_cache.json'

# ── FTP Neubox ────────────────────────────────────────────────
FTP_HOST = 'ftp.nexus-core.com.mx'
FTP_USER = 'servicios@nexus-core.com.mx'
FTP_PASS = os.environ.get('FTP_PASS', 'S3rvic3$1984*$*')
FTP_DIR  = '/public_html/data/'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/121.0.0.0 Safari/537.36'
    ),
    'Accept':  'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer': 'https://official.nba.com/',
}

COL_BOUNDS = [
    ('date',    0,   100),
    ('time',  100,   190),
    ('matchup',190,  250),
    ('team',  250,   410),
    ('player',410,   570),
    ('status',570,   650),
    ('reason',650,  9999),
]

KEEP_STATUSES = {'OUT', 'Questionable', 'Probable', 'Doubtful', 'Day-To-Day'}

MAX_HISTORY          = 50
MAX_DOWNLOAD_RETRIES = 3
RETRY_WAIT_SECONDS   = 10

SKIP_PATTERNS = re.compile(
    r'^(Injury\s*Report|Page\s*\d|Game\s*Date|GameDate|GameTime|Matchup|'
    r'Team|PlayerName|CurrentStatus|Reason|\d{1,2}/\d{1,2}/\d{2,4}|PM|AM)$',
    re.IGNORECASE
)

def log(msg):
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}', flush=True)

def get_et_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        pass
    utc_now = datetime.now(timezone.utc)
    def nth_sunday(year, month, n):
        d = datetime(year, month, 1)
        count = 0
        while True:
            if d.weekday() == 6:
                count += 1
                if count == n:
                    return d
            d += timedelta(days=1)
    year      = utc_now.year
    dst_start = nth_sunday(year, 3,  2).replace(hour=7,  tzinfo=timezone.utc)
    dst_end   = nth_sunday(year, 11, 1).replace(hour=6,  tzinfo=timezone.utc)
    is_edt    = dst_start <= utc_now < dst_end
    offset    = timedelta(hours=-4 if is_edt else -5)
    log(f'  Zona horaria fallback: {"EDT (UTC-4)" if is_edt else "EST (UTC-5)"}')
    return (utc_now + offset).replace(tzinfo=timezone(offset))

def col_of(x0):
    for name, lo, hi in COL_BOUNDS:
        if lo <= x0 < hi:
            return name
    return None

def group_by_rows(words, tol=4):
    by_y = {}
    for w in words:
        y   = round(w['top'] / tol) * tol
        col = col_of(w['x0'])
        if col is None:
            continue
        if y not in by_y:
            by_y[y] = {n: '' for n, _, __ in COL_BOUNDS}
            by_y[y]['y'] = y
        by_y[y][col] = (by_y[y][col] + ' ' + w['text']).strip()

    rows = sorted(by_y.values(), key=lambda r: r['y'])

    def is_reason_only(r):
        return (bool(r.get('reason', '').strip()) and
                not bool(r.get('player', '').strip()) and
                not bool(r.get('status', '').strip()))

    def has_player_status(r):
        return (bool(r.get('player', '').strip()) and
                bool(r.get('status', '').strip()))

    merged = []
    i = 0
    while i < len(rows):
        row = rows[i]

        if is_reason_only(row):
            if i + 1 < len(rows) and has_player_status(rows[i + 1]):
                nxt = rows[i + 1]
                existing = nxt.get('reason', '').strip()
                nxt['reason'] = (row['reason'] + ' ' + existing).strip() if existing else row['reason']
                log(f'  [PRE]  {nxt["player"]}: "{row["reason"]}"')
                i += 1
                continue
            if merged:
                merged[-1]['reason'] = (merged[-1].get('reason', '') + ' ' + row['reason']).strip()
                log(f'  [POST] {merged[-1].get("player","?")} += "{row["reason"]}"')
            i += 1
            continue

        merged.append(row)
        i += 1

        while i < len(rows) and is_reason_only(rows[i]):
            if i + 1 < len(rows) and has_player_status(rows[i + 1]):
                break
            merged[-1]['reason'] = (merged[-1].get('reason', '') + ' ' + rows[i]['reason']).strip()
            log(f'  [POST] {merged[-1].get("player","?")} += "{rows[i]["reason"]}"')
            i += 1

    return merged

def merge_wrapped_names(rows):
    merged  = []
    pending = None

    for row in rows:
        player = row.get('player', '').strip()
        status = row.get('status', '').strip()

        if player.endswith(',') and not status:
            if pending:
                merged.append(pending)
            pending = row
            continue

        if pending and player and ',' not in player and status:
            combined = pending['player'].rstrip(',').strip() + ', ' + player
            fused = dict(row)
            fused['player'] = combined
            for field in ('team', 'matchup', 'reason'):
                if not fused.get(field) and pending.get(field):
                    fused[field] = pending[field]
            log(f'  [WRAP] Fusionado: "{pending["player"]}" + "{player}" → "{combined}"')
            merged.append(fused)
            pending = None
            continue

        if pending:
            merged.append(pending)
            pending = None

        merged.append(row)

    if pending:
        merged.append(pending)

    return merged

def normalize_team_name(raw):
    if not raw:
        return ''
    spaced = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', raw.strip())
    return re.sub(r'\s+', ' ', spaced).strip()

def normalize_status(s):
    sl = s.strip().lower()
    if sl == 'out':           return 'OUT'
    if 'questionable' in sl:  return 'Questionable'
    if 'probable'     in sl:  return 'Probable'
    if 'doubtful'     in sl:  return 'Doubtful'
    if 'day-to-day'   in sl:  return 'Day-To-Day'
    if 'available'    in sl:  return 'Available'
    return s.strip()

# ── FIX v10.5: sufijo pegado al apellido sin espacio ──────────────────────────
# Antes:  r'\s+(Jr\.?|Sr\.?|III|II|IV|V)\s*$'   → requería espacio previo
# Ahora:  r'\s*(Jr\.?|Sr\.?|III|II|IV|V)\s*$'   → acepta con O sin espacio
# Esto corrige: "ButlerIII" → "BUTLER III", "LivelyII" → "LIVELY II",
#               "SmithJr." → "SMITH JR.", "PaytonII" → "PAYTON II"
# Nombres sin sufijo (James, Curry, etc.) no se ven afectados.
_SUFFIX_RE = re.compile(
    r'\s*(Jr\.?|Sr\.?|III|II|IV|V)\s*$', re.IGNORECASE
)

def normalize_player_name(raw):
    raw = raw.strip()
    if not raw:
        return ''
    if ',' in raw:
        parts  = raw.split(',', 1)
        last   = parts[0].strip()
        first  = parts[1].strip() if len(parts) > 1 else ''

        suffix_match = _SUFFIX_RE.search(last)
        suffix = ''
        if suffix_match:
            suffix = ' ' + suffix_match.group(1).strip()
            last   = last[:suffix_match.start()].strip()

        full = f'{first} {last}{suffix}'.strip()
    else:
        full = raw
    return re.sub(r'\s+', ' ', full).upper()

def is_valid_team(team_str):
    t = team_str.strip()
    if not t or len(t) < 3:
        return False
    if re.match(
        r'^(Injury\s*Report|Page\s*\d|Game\s*Date|GameDate|\d{1,2}/\d{1,2})',
        t, re.IGNORECASE
    ):
        return False
    if re.search(r'\d', t) and len(t) < 8:
        return False
    return True

def parse_pdf(pdf_bytes):
    injuries    = {}
    cur_team    = ''
    cur_matchup = ''

    GLEAGUE_KEYS = ('gleague', 'g league', 'two-way', 'two way', 'assignment')

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        log(f'  PDF: {len(pdf.pages)} páginas')
        for page_num, page in enumerate(pdf.pages, 1):
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            if not words:
                log(f'  Página {page_num}: sin palabras, saltando')
                continue

            raw_rows = group_by_rows(words, tol=4)
            rows     = merge_wrapped_names(raw_rows)

            for row in rows:
                team_cell = row.get('team', '').strip()
                if is_valid_team(team_cell):
                    cur_team = normalize_team_name(team_cell)

                matchup_cell = row.get('matchup', '').strip()
                if matchup_cell and '@' in matchup_cell:
                    cur_matchup = matchup_cell

                player_raw = row.get('player', '').strip()
                status_raw = row.get('status', '').strip()

                if not player_raw or not status_raw:
                    continue
                if SKIP_PATTERNS.match(player_raw):
                    continue
                if SKIP_PATTERNS.match(status_raw):
                    continue
                if re.match(r'^\d{2}/\d{2}/\d{2}', player_raw):
                    continue
                if 'Injury Report' in row.get('date', ''):
                    continue

                status = normalize_status(status_raw)
                if status not in KEEP_STATUSES:
                    continue

                key = normalize_player_name(player_raw)
                if not key or len(key) < 3:
                    continue

                reason = row.get('reason', '').strip()

                if any(k in reason.lower() for k in GLEAGUE_KEYS):
                    continue

                injuries[key] = {
                    'status':   status,
                    'team':     cur_team,
                    'matchup':  cur_matchup,
                    'reason':   reason,
                    'raw_name': player_raw,
                }

    log(f'  Parseados: {len(injuries)} jugadores')
    return injuries

def build_pdf_url(et_now, minutes_back=0):
    et_slot     = et_now - timedelta(minutes=5 + minutes_back)
    minute_slot = (et_slot.minute // 15) * 15
    slot_time   = et_slot.replace(minute=minute_slot, second=0, microsecond=0)
    hour24      = slot_time.hour
    minute      = slot_time.minute
    ampm        = 'AM' if hour24 < 12 else 'PM'
    hour12      = hour24 % 12 or 12
    url = (
        f'https://ak-static.cms.nba.com/referee/injury/'
        f'Injury-Report_{slot_time.strftime("%Y-%m-%d")}'
        f'_{hour12:02d}_{minute:02d}{ampm}.pdf'
    )
    return url, slot_time

def download_pdf(et_now):
    slots = [0, 15, 30, 45, 60]
    for minutes_back in slots:
        url, slot_time = build_pdf_url(et_now, minutes_back)
        log(f'  Slot {slot_time.strftime("%H:%M")} → {url}')
        for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                if resp.status_code == 200:
                    log(f'  ✅ Descargado ({len(resp.content):,} bytes) intento {attempt}')
                    return resp.content, url
                if resp.status_code == 404:
                    log(f'  404 — no existe, probando slot anterior')
                    break
                if resp.status_code in (403, 429):
                    log(f'  HTTP {resp.status_code} — esperando {RETRY_WAIT_SECONDS}s...')
                    if attempt < MAX_DOWNLOAD_RETRIES:
                        time.sleep(RETRY_WAIT_SECONDS)
                    continue
                log(f'  HTTP {resp.status_code} intento {attempt}')
                if attempt < MAX_DOWNLOAD_RETRIES:
                    time.sleep(RETRY_WAIT_SECONDS)
            except requests.exceptions.Timeout:
                log(f'  Timeout intento {attempt}')
                if attempt < MAX_DOWNLOAD_RETRIES:
                    time.sleep(RETRY_WAIT_SECONDS)
            except requests.exceptions.ConnectionError as e:
                log(f'  Error conexión intento {attempt}: {e}')
                if attempt < MAX_DOWNLOAD_RETRIES:
                    time.sleep(RETRY_WAIT_SECONDS)

    log('  ❌ No se pudo descargar el PDF en ningún slot.')
    return None, None

def load_cache():
    if not os.path.exists(OUTPUT_FILE):
        return {}, []
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('injuries', {}), data.get('change_history', [])
    except Exception as e:
        log(f'  Error cargando cache: {e}')
        return {}, []

def clean_history(history, fresh_injuries=None):
    fixed = 0
    for entry in history:
        if entry.get('team') == 'Injury Report:':
            player       = entry.get('player', '')
            correct_team = ''
            if fresh_injuries and player in fresh_injuries:
                correct_team = fresh_injuries[player].get('team', '')
            if correct_team and correct_team != 'Injury Report:':
                log(f'  [HIST] Corregido: {player} → "{correct_team}"')
                entry['team'] = correct_team
                fixed += 1
            else:
                log(f'  [HIST] {player} tiene "Injury Report:" sin team correcto disponible')
    if fixed:
        log(f'  {fixed} entradas del historial corregidas')
    return history

def detect_changes(prev, curr):
    SEVERITY = {
        'OUT': 4, 'Doubtful': 3, 'Day-To-Day': 3,
        'Questionable': 2, 'Probable': 1, None: 0
    }
    changes = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    for player in sorted(set(prev) | set(curr)):
        ps   = prev.get(player, {}).get('status') if player in prev else None
        cs   = curr.get(player, {}).get('status') if player in curr else None
        if ps == cs:
            continue

        team = (curr.get(player) or prev.get(player) or {}).get('team', '')

        if ps is None:
            ctype = 'NUEVO'
        elif cs is None:
            ctype = 'RETIRADO'
        elif SEVERITY.get(cs, 0) > SEVERITY.get(ps, 0):
            ctype = 'EMPEORÓ'
        elif SEVERITY.get(cs, 0) < SEVERITY.get(ps, 0):
            ctype = 'MEJORÓ'
        else:
            ctype = 'CAMBIO'

        changes.append({
            'timestamp':   now_str,
            'player':      player,
            'team':        team,
            'from_status': ps or '—',
            'to_status':   cs or 'REMOVIDO',
            'type':        ctype,
            'emoji':       ctype,
        })
        log(f'  {ctype}: {player} ({team}) {ps or "—"} → {cs or "REMOVIDO"}')

    return changes

def save_cache(injuries, pdf_url, new_changes, prev_history):
    out  = [k for k, v in injuries.items() if v['status'] == 'OUT']
    q    = [k for k, v in injuries.items() if v['status'] == 'Questionable']
    p    = [k for k, v in injuries.items() if v['status'] == 'Probable']
    d    = [k for k, v in injuries.items() if v['status'] == 'Doubtful']
    dtd  = [k for k, v in injuries.items() if v['status'] == 'Day-To-Day']

    history = (new_changes + prev_history)[:MAX_HISTORY]

    data = {
        'updated_at':           datetime.now(timezone.utc).isoformat(),
        'updated_at_ct':        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_pdf':           pdf_url or '',
        'total_entries':        len(injuries),
        'injuries':             injuries,
        'out_players':          out,
        'questionable_players': q,
        'probable_players':     p,
        'doubtful_players':     d,
        'day_to_day_players':   dtd,
        'last_run_changes':     new_changes,
        'change_history':       history,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    log(f'  Guardado → {OUTPUT_FILE}')
    log(f'  OUT:          {len(out)}  → {out[:6]}')
    log(f'  Questionable: {len(q)}')
    log(f'  Probable:     {len(p)}')
    log(f'  Doubtful:     {len(d)}')
    if dtd:
        log(f'  Day-To-Day:   {len(dtd)}')

def run():
    log('=' * 55)
    log('NBA INJURY SCRAPER 2026 · v10.5')
    log('=' * 55)

    try:
        prev_injuries, prev_history = load_cache()
        log(f'Cache anterior: {len(prev_injuries)} jugadores')

        et_now = get_et_now()
        log(f'Hora ET actual: {et_now.strftime("%Y-%m-%d %H:%M")}')

        pdf_bytes, pdf_url = download_pdf(et_now)

        if pdf_bytes is None:
            log('PDF no disponible — cache anterior se mantiene sin cambios.')
            return

        new_injuries = parse_pdf(pdf_bytes)

        log('Verificando historial...')
        prev_history = clean_history(prev_history, fresh_injuries=new_injuries)

        log('-' * 40)
        if prev_injuries:
            log('Comparando con cache anterior...')
            changes = detect_changes(prev_injuries, new_injuries)
            if not changes:
                log('  Sin cambios desde la última corrida')
        else:
            log('Primera corrida — sin cache previo')
            changes = []

        log('-' * 40)
        save_cache(new_injuries, pdf_url, changes, prev_history)
        ftp_upload_injury()
        log('✅ Listo!')

    except Exception as e:
        log(f'ERROR CRÍTICO: {e}')
        import traceback
        traceback.print_exc()

def ftp_upload_injury():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.set_pasv(True)
        ftp.cwd(FTP_DIR)
        with open(OUTPUT_FILE, 'rb') as f:
            ftp.storbinary('STOR injury_cache.json', f)
        ftp.quit()
        log('✅ FTP injury_cache.json subido a Neubox')
    except Exception as e:
        log(f'❌ FTP ERROR: {e}')

if __name__ == '__main__':
    run()