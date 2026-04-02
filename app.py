from flask import (Flask, render_template, request, make_response,
                   redirect, url_for, Response, session, jsonify)
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import os
import csv
import io
import logging
import threading
import time
import secrets
from functools import wraps

app = Flask(__name__)
@app.template_filter('nicedate')
def nicedate_filter(s):
    """Convert '2026-03-30' to 'March 30, 2026'."""
    try:
        from datetime import datetime as _dt
        d = _dt.strptime(str(s).strip()[:10], '%Y-%m-%d')
        return d.strftime('%B %d, %Y').replace(' 0', ' ')
    except Exception:
        return str(s) if s else '—'

@app.template_filter('money')
def money_filter(s):
    """Format a number as $##.## — handles floats, strings, None, Undefined."""
    try:
        if s is None or str(s).strip() == '':
            return '0.00'
        return '%.2f' % float(s)
    except Exception:
        return '0.00'

@app.template_filter('commafy')
def commafy_filter(s):
    """Format a number with commas: 90000 → 90,000."""
    try:
        if s is None or str(s).strip() == '':
            return '0'
        return '{:,.0f}'.format(float(s))
    except Exception:
        return '0'

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
GOOGLE_SCRIPT_URL = os.environ.get(
    'GOOGLE_SCRIPT_URL',
    "https://script.google.com/macros/s/AKfycbxKVyW7sguwUq3TYsk-xtIF2fLicefaxTwl_PHjQVjt5-OiBarPQ_nXb_0H927NXAMG0w/exec"
)
APP_BASE_URL    = os.environ.get('APP_BASE_URL', 'https://betterday-app.onrender.com')
ADMIN_PASSWORD  = os.environ.get('ADMIN_PASSWORD',   'betterday2024')
HELCIM_API_TOKEN = os.environ.get('HELCIM_API_TOKEN', '')
SMTP_EMAIL       = os.environ.get('SMTP_EMAIL', '')
SMTP_PASSWORD    = os.environ.get('SMTP_PASSWORD', '')
app.secret_key  = os.environ.get('FLASK_SECRET_KEY', 'bd-dev-secret-change-in-prod')
CULINARY_SYNC_KEY = os.environ.get('CULINARY_SYNC_KEY', 'bd-culinary-sync-2026')

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# MAGIC LINK TOKEN STORE  (Flask-side, bypasses GAS verify_magic_token)
# ─────────────────────────────────────────────────────────────
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def _send_email(to_email, subject, html_body, plain_body=None):
    """Send an email via Gmail SMTP. Returns True on success."""
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        log.warning('SMTP not configured — skipping email to %s', to_email)
        return False
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = f'BetterDay <{SMTP_EMAIL}>'
        msg['To'] = to_email
        msg['Subject'] = subject
        if plain_body:
            msg.attach(MIMEText(plain_body, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=10) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.sendmail(SMTP_EMAIL, to_email, msg.as_string())
        log.info('Email sent to %s', to_email)
        return True
    except Exception as ex:
        log.warning('SMTP email failed to %s: %s', to_email, ex)
        return False


def _build_magic_link_email(sign_in_url, header_bg='#4ea2fd', header_label='FOR WORK',
                            btn_text='Sign in to BetterDay →', btn_bg='#4ea2fd'):
    """Build the branded magic-link HTML email body."""
    logo_url = f'{APP_BASE_URL}/static/Cream%20Logo.png'
    return (
        "<!DOCTYPE html><html><body style='margin:0;padding:0;background:#f4ede3;"
        "font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif;'>"
        "<table width='100%' cellpadding='0' cellspacing='0' style='background:#f4ede3;padding:40px 16px;'>"
        "<tr><td align='center'>"
        "<table width='480' cellpadding='0' cellspacing='0' style='max-width:480px;width:100%;'>"
        f"<tr><td style='background:{header_bg};border-radius:16px 16px 0 0;padding:28px 32px;text-align:center;'>"
        f"<img src='{logo_url}' alt='BetterDay' style='height:36px;' />"
        f"<div style='font-size:.65rem;color:rgba(255,255,255,.75);letter-spacing:2px;"
        f"text-transform:uppercase;margin-top:6px;'>{header_label}</div>"
        "</td></tr>"
        "<tr><td style='background:#ffffff;padding:36px 32px 28px;'>"
        "<p style='font-size:1.15rem;font-weight:800;color:#0d2030;margin:0 0 10px;'>"
        "Your sign-in link is ready</p>"
        "<p style='font-size:.9rem;color:#50657a;line-height:1.65;margin:0 0 28px;'>"
        "Click the button below to sign in — no password needed. "
        "This link expires in <strong>15 minutes</strong> and can only be used once.</p>"
        f"<a href='{sign_in_url}' style='display:block;background:{btn_bg};color:#ffffff;"
        "text-decoration:none;padding:16px 24px;border-radius:12px;text-align:center;"
        f"font-weight:700;font-size:1rem;letter-spacing:0.2px;'>{btn_text}</a>"
        "</td></tr>"
        "<tr><td style='background:#f9f5f0;border-radius:0 0 16px 16px;padding:20px 32px;"
        "border-top:1px solid #e8e0d5;'>"
        "<p style='font-size:.75rem;color:#9aabb8;margin:0;line-height:1.6;'>"
        "If you didn't request this, you can safely ignore it — your account is secure.<br>"
        "Questions? Reply to this email.</p>"
        "</td></tr></table></td></tr></table></body></html>"
    )

_token_store      = {}   # token → {email, company_id, created_at, used}
_token_store_lock = threading.Lock()
_TOKEN_TTL        = 900  # 15 minutes
_menu_cache       = {}   # 'menu_{anchor}' → {data, ts}
_available_meals  = {}   # 'YYYY-MM-DD' → ['#509', '#388', ...]  pushed from culinary-ops

def _store_magic_token(token, email, company_id):
    with _token_store_lock:
        _token_store[token] = {
            'email': email.strip().lower(),
            'company_id': company_id.strip().upper(),
            'created_at': time.time(),
            'used': False
        }

def _verify_magic_token_flask(token):
    """Verify a magic link token stored by Flask. Returns (email, company_id) or None."""
    with _token_store_lock:
        entry = _token_store.get(token)
        if not entry:
            return None
        if entry['used']:
            return None
        if time.time() - entry['created_at'] > _TOKEN_TTL:
            return None
        entry['used'] = True
        return entry['email'], entry['company_id']

# ─────────────────────────────────────────────────────────────
# COMPANY LOOKUP CACHE  (avoids GAS cold-start on every keystroke)
# ─────────────────────────────────────────────────────────────
_company_cache      = {}   # CompanyID.upper() → {data, ts}
_company_cache_lock = threading.Lock()
_COMPANY_TTL        = 600  # 10 minutes
_warmup_done        = False

@app.before_request
def _startup_warmup():
    global _warmup_done
    if not _warmup_done:
        _warmup_done = True
        threading.Thread(target=_warmup_gas, daemon=True).start()


def _cached_get_company(company_id):
    code = company_id.strip().upper()
    with _company_cache_lock:
        entry = _company_cache.get(code)
        if entry and time.time() - entry['ts'] < _COMPANY_TTL:
            return entry['data']
    result = None
    try:
        r = requests.post(GOOGLE_SCRIPT_URL,
                          json={'action': 'get_company', 'company_id': code},
                          timeout=15)
        result = r.json()
    except Exception as ex:
        log.warning('company lookup error (%s): %s', code, ex)
        return None
    with _company_cache_lock:
        _company_cache[code] = {'data': result, 'ts': time.time()}
    return result


def _warmup_gas():
    """Pre-load all companies into the Flask cache so lookups are instant."""
    try:
        r = requests.post(GOOGLE_SCRIPT_URL,
                          json={'action': 'get_all_companies'},
                          timeout=25)
        data = r.json()
        companies = data.get('companies') or []
        if companies:
            now = time.time()
            with _company_cache_lock:
                for c in companies:
                    cid = str(c.get('CompanyID', '')).strip().upper()
                    if cid:
                        _company_cache[cid] = {'data': {'found': True, 'company': c}, 'ts': now}
            log.info('Warmed company cache: %d companies', len(companies))
            return
    except Exception:
        pass
    # Fallback: fire a cheap single-company call just to wake GAS
    try:
        requests.post(GOOGLE_SCRIPT_URL,
                      json={'action': 'get_company', 'company_id': '__warmup__'},
                      timeout=20)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# ADMIN AUTH
# ─────────────────────────────────────────────────────────────
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login', next=request.path))
        return f(*args, **kwargs)
    return decorated


@app.route('/admin-login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            next_url = request.args.get('next') or url_for('bd_admin_dashboard')
            return redirect(next_url)
        error = 'Incorrect password.'
    return render_template('admin_login.html', error=error)


@app.route('/admin-logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))


@app.route('/bd-admin/clear-cache', methods=['POST'])
@admin_required
def clear_cache():
    """Clear all Flask caches — forces fresh GAS lookups."""
    with _company_cache_lock:
        _company_cache.clear()
    _menu_cache.clear()
    threading.Thread(target=_warmup_gas, daemon=True).start()
    return jsonify({'success': True, 'message': 'Cache cleared and re-warming'})


# ─────────────────────────────────────────────────────────────
# GAS PROXY  — single endpoint, keeps the Apps Script URL
#              server-side and out of the browser
# ─────────────────────────────────────────────────────────────
@app.route('/api/gas', methods=['POST'])
def gas_proxy():
    payload = request.get_json(force=True) or {}

    # ── create_magic_token: Flask generates token, sends email via SMTP, stores in GAS async ──
    if payload.get('action') == 'create_magic_token':
        token      = secrets.token_hex(32)
        company_id = str(payload.get('company_id', '')).strip().upper()
        email      = str(payload.get('email', '')).strip().lower()
        sign_in_url = f"{APP_BASE_URL}/work?token={token}&co={company_id}"
        _store_magic_token(token, email, company_id)

        # Send email directly from Flask (fast — no GAS dependency)
        html_body = _build_magic_link_email(sign_in_url)
        email_sent = _send_email(email, 'Your BetterDay sign-in link', html_body,
                                  plain_body=f'Sign in here: {sign_in_url}\n\nExpires in 15 minutes.')
        if email_sent:
            # Store token in GAS in the background (for audit trail only — not blocking)
            def _bg_store():
                try:
                    _gas_post({'action': 'create_magic_token', 'email': email,
                               'company_id': company_id, 'token_override': token,
                               'sign_in_url': sign_in_url, 'skip_email': True}, timeout=15)
                except Exception:
                    pass
            threading.Thread(target=_bg_store, daemon=True).start()
            return jsonify({'success': True})
        # SMTP not configured or failed — fall through to GAS to send email
        payload['token_override'] = token
        payload['sign_in_url'] = sign_in_url

    # ── verify_magic_token: Flask checks its own store (fast, no GAS round-trip) ──
    elif payload.get('action') == 'verify_magic_token':
        token  = str(payload.get('token', '')).strip()
        result = _verify_magic_token_flask(token)
        if result:
            email, company_id = result
            # Get employee + company data from GAS
            emp_data = _gas_post({'action': 'get_employee_by_email',
                                  'email': email, 'company_id': company_id}, timeout=12)
            comp_data = _cached_get_company(company_id)
            employee = emp_data.get('employee') if emp_data and emp_data.get('found') else None
            company  = comp_data.get('company') if comp_data and comp_data.get('found') else None
            if employee:
                return jsonify({'valid': True, 'employee': employee, 'company': company})
        # Token not in Flask store — fall through to GAS (handles tokens from old emails)
        # (fall through to the requests.post below)

    if payload.get('action') == 'get_menu':
        anchor = payload.get('sunday_anchor', '')
        cache_key = f'menu_{anchor}'
        cached = _menu_cache.get(cache_key)
        if cached and (time.time() - cached['ts'] < 600):
            data = cached['data']
            if anchor in _available_meals:
                allowed = set(_available_meals[anchor])
                data = {
                    'meat':  [m for m in (data.get('meat')  or []) if str(m.get('MealID','')).strip() in allowed],
                    'vegan': [m for m in (data.get('vegan') or []) if str(m.get('MealID','')).strip() in allowed],
                }
            return jsonify(data), 200
        try:
            r = requests.post(GOOGLE_SCRIPT_URL, json=payload, timeout=20)
            data = r.json()
            if r.status_code == 200 and (data.get('meat') or data.get('vegan')):
                _menu_cache[cache_key] = {'data': data, 'ts': time.time()}
            if anchor in _available_meals and r.status_code == 200:
                allowed = set(_available_meals[anchor])
                data = {
                    'meat':  [m for m in (data.get('meat')  or []) if str(m.get('MealID','')).strip() in allowed],
                    'vegan': [m for m in (data.get('vegan') or []) if str(m.get('MealID','')).strip() in allowed],
                }
            return jsonify(data), r.status_code
        except requests.Timeout:
            if cached:
                return jsonify(cached['data']), 200
            return jsonify({'error': 'Menu loading timed out — please try again.'}), 504
        except Exception as ex:
            log.error('Menu fetch error: %s', ex)
            if cached:
                return jsonify(cached['data']), 200
            return jsonify({'error': 'Could not load menu.'}), 500


    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json=payload, timeout=15)
        return jsonify(r.json()), r.status_code
    except requests.Timeout:
        log.warning('GAS timeout: action=%s', payload.get('action'))
        return jsonify({'error': 'Request timed out — please try again.'}), 504
    except Exception as ex:
        log.error('GAS proxy error: %s', ex)
        return jsonify({'error': 'Server error — please try again.'}), 500


# ─────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────
def _gas_post(payload, timeout=10):
    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json=payload, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception as ex:
        log.warning('GAS POST error (action=%s): %s', payload.get('action'), ex)
        return None


def _current_monday():
    """Always return this week's Monday — never a hardcoded date."""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=today.weekday())


def get_nice_date(date_str):
    try:
        dt = datetime.strptime(str(date_str).split('T')[0], '%Y-%m-%d')
        return dt.strftime('%A, %b %d')
    except Exception:
        return date_str


def format_week_header(date_str):
    try:
        dt = datetime.strptime(str(date_str), '%Y-%m-%d')
        return dt.strftime('%b %d, %Y')
    except Exception:
        return date_str


def get_sunday_anchor(delivery_date_str):
    try:
        clean = str(delivery_date_str).split('T')[0]
        dt  = datetime.strptime(clean, '%Y-%m-%d')
        sub = (dt.weekday() + 1) % 7 or 7
        return (dt - timedelta(days=sub)).strftime('%Y-%m-%d')
    except Exception:
        return None


def get_deadline_obj(delivery_date_str):
    try:
        clean = str(delivery_date_str).split('T')[0]
        dt  = datetime.strptime(clean, '%Y-%m-%d')
        sub = (dt.weekday() - 2) % 7
        if sub <= 2:
            sub += 7
        return (dt - timedelta(days=sub)).replace(hour=16, minute=0, second=0)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# ROOT REDIRECT → Corporate ordering app
# ─────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return redirect('/work')


# ─────────────────────────────────────────────────────────────
# LEGACY REDIRECTS (old URLs → new admin dashboard)
# ─────────────────────────────────────────────────────────────
@app.route('/BD-Admin')
@admin_required
def bd_admin():
    return redirect(url_for('bd_admin_dashboard'))


# ─────────────────────────────────────────────────────────────
# BD ADMIN DASHBOARD  (new — corporate + system-wide view)
# ─────────────────────────────────────────────────────────────
@app.route('/bd-admin/dashboard')
@admin_required
def bd_admin_dashboard():
    import json as _json
    from collections import defaultdict

    # ── Companies from cache (instant) ──────────────────────
    with _company_cache_lock:
        companies = [
            entry['data']['company']
            for entry in _company_cache.values()
            if entry.get('data') and entry['data'].get('found') and entry['data'].get('company')
        ]
    companies.sort(key=lambda c: (c.get('CompanyName') or c.get('CompanyID') or '').lower())

    # ── Parallel GAS calls ───────────────────────────────────
    results = {}
    def _fetch(key, payload, timeout):
        results[key] = _gas_post(payload, timeout=timeout) or {}

    threads = [
        threading.Thread(target=_fetch, args=('orders',   {'action': 'get_corporate_orders'}, 20)),
        threading.Thread(target=_fetch, args=('invoices', {'action': 'get_all_invoices'},     12)),
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    invoices = results.get('invoices', {}).get('invoices', [])
    raw = results.get('orders', {})
    if not isinstance(raw, list): raw = []

    # ── Group meal rows by OrderID ───────────────────────────
    order_map = {}
    for row in raw:
        oid = str(row.get('OrderID') or '').strip()
        if not oid:
            oid = f"{row.get('EmployeeEmail','anon')}-{row.get('SundayAnchor','')}"
        if oid not in order_map:
            order_map[oid] = {
                'order_id':       oid,
                'company_id':     str(row.get('CompanyID', '') or '').upper(),
                'employee_name':  row.get('EmployeeName', ''),
                'employee_email': row.get('EmployeeEmail', ''),
                'delivery_date':  str(row.get('DeliveryDate', '') or ''),
                'sunday_anchor':  str(row.get('SundayAnchor', '') or ''),
                'status':         row.get('Status', ''),
                'meals':          [],
                'emp_total':      0.0,
                'co_total':       0.0,
                'bd_total':       0.0,
            }
        rec = order_map[oid]
        emp = float(row.get('EmployeePrice')   or 0)
        co  = float(row.get('CompanyCoverage') or 0)
        bd  = float(row.get('BDCoverage')      or 0)
        rec['meals'].append({'dish_name': row.get('DishName', ''), 'tier': row.get('Tier', ''),
                             'emp_price': emp, 'co_coverage': co, 'bd_coverage': bd})
        rec['emp_total'] += emp
        rec['co_total']  += co
        rec['bd_total']  += bd

    all_orders = sorted(order_map.values(), key=lambda o: o['delivery_date'], reverse=True)

    # ── Per-company stats ────────────────────────────────────
    co_stats = {}
    for o in all_orders:
        cid = o['company_id']
        if not cid:
            continue
        if cid not in co_stats:
            co_stats[cid] = {'meals': 0, 'orders': 0, 'employees': set(),
                             'co_spend': 0.0, 'bd_spend': 0.0, 'emp_spend': 0.0, 'last_order': ''}
        s = co_stats[cid]
        s['meals']    += len(o['meals'])
        s['orders']   += 1
        s['employees'].add(o['employee_email'])
        s['co_spend'] += o['co_total']
        s['bd_spend'] += o['bd_total']
        s['emp_spend'] += o['emp_total']
        if o['delivery_date'] > s['last_order']:
            s['last_order'] = o['delivery_date']
    for s in co_stats.values():
        s['employee_count'] = len(s['employees'])
        del s['employees']

    # ── Group by week ────────────────────────────────────────
    week_map = defaultdict(list)
    for o in all_orders:
        if o['sunday_anchor']:
            week_map[o['sunday_anchor']].append(o)

    sorted_weeks = []
    for anchor in sorted(week_map.keys(), reverse=True):
        try:
            monday = datetime.strptime(anchor, '%Y-%m-%d') + timedelta(days=1)
            nice_label = f"Week of {monday.strftime('%b %d, %Y')}"
        except Exception:
            nice_label = anchor
        wo = week_map[anchor]
        sorted_weeks.append({
            'anchor':      anchor,
            'label':       nice_label,
            'orders':      wo,
            'order_count': len(wo),
            'meal_count':  sum(len(o['meals']) for o in wo),
            'emp_spend':   round(sum(o['emp_total'] for o in wo), 2),
            'co_spend':    round(sum(o['co_total']  for o in wo), 2),
            'bd_spend':    round(sum(o['bd_total']  for o in wo), 2),
        })

    empty_week = {'orders': [], 'order_count': 0, 'meal_count': 0,
                  'emp_spend': 0.0, 'co_spend': 0.0, 'bd_spend': 0.0, 'label': '—', 'anchor': ''}
    active_week = sorted_weeks[0] if sorted_weeks else empty_week

    # ── Week snapshot by company ─────────────────────────────
    week_by_co = {}
    for o in active_week['orders']:
        cid = o['company_id']
        if not cid: continue
        if cid not in week_by_co:
            week_by_co[cid] = {'meals': 0, 'employees': set(), 'co_spend': 0.0, 'bd_spend': 0.0}
        week_by_co[cid]['meals']    += len(o['meals'])
        week_by_co[cid]['employees'].add(o['employee_email'])
        week_by_co[cid]['co_spend'] += o['co_total']
        week_by_co[cid]['bd_spend'] += o['bd_total']
    week_snapshot = sorted([
        {'company_id': cid, 'meals': s['meals'],
         'employee_count': len(s['employees']),
         'co_spend': round(s['co_spend'], 2), 'bd_spend': round(s['bd_spend'], 2)}
        for cid, s in week_by_co.items()
    ], key=lambda x: x['meals'], reverse=True)

    # ── System-wide stats ────────────────────────────────────
    total_meals            = sum(len(o['meals']) for o in all_orders)
    total_co_spend         = round(sum(o['co_total'] for o in all_orders), 2)
    total_bd_spend         = round(sum(o['bd_total'] for o in all_orders), 2)
    total_emp_spend        = round(sum(o['emp_total'] for o in all_orders), 2)
    total_unique_employees = len(set(o['employee_email'] for o in all_orders if o['employee_email']))
    total_companies        = len(companies)
    active_companies_week  = len(set(o['company_id'] for o in active_week['orders']))
    def _inv_status(inv):
        s = inv.get('Status') or inv.get('status') or 'pending'
        return str(s).strip().lower()
    pending_invoices_value = round(sum(
        float(inv.get('AmountDue') or inv.get('amountDue') or inv.get('companyOwed') or inv.get('CompanyOwed') or 0)
        for inv in invoices if _inv_status(inv) in ('pending', 'sent', 'overdue')
    ), 2)
    pending_invoices_count = sum(1 for inv in invoices if _inv_status(inv) in ('pending', 'sent', 'overdue'))

    # This week stats
    week_orders      = active_week['orders']
    week_order_count = len(week_orders)
    week_meal_count  = sum(len(o['meals']) for o in week_orders)
    week_employees   = len(set(o['employee_email'] for o in week_orders if o['employee_email']))
    week_revenue     = round(sum(o['emp_total'] + o['co_total'] for o in week_orders), 2)

    # All-time revenue & avg order value
    total_revenue    = round(total_emp_spend + total_co_spend, 2)
    total_orders     = len(all_orders)
    avg_order_value  = round(total_revenue / total_orders, 2) if total_orders else 0

    # Growth / fun stats
    # Top company (most meals this month)
    from collections import Counter
    co_meal_counts = Counter()
    for o in all_orders:
        co_meal_counts[o['company_id']] += len(o['meals'])
    top_company_id = co_meal_counts.most_common(1)[0][0] if co_meal_counts else ''
    top_company_meals = co_meal_counts.get(top_company_id, 0)
    top_company_revenue = round(sum(o['co_total'] + o['emp_total'] for o in all_orders if o['company_id'] == top_company_id), 2)

    # Most popular meal
    dish_counts = Counter()
    dish_revenue = {}
    for o in all_orders:
        for m in o['meals']:
            if m['dish_name']:
                dish_counts[m['dish_name']] += 1
                dish_revenue[m['dish_name']] = dish_revenue.get(m['dish_name'], 0) + m['emp_price'] + m['co_coverage']
    top_meal = dish_counts.most_common(1)[0][0] if dish_counts else '—'
    top_meal_count = dish_counts.get(top_meal, 0)
    top_meal_revenue = round(dish_revenue.get(top_meal, 0), 2)
    top_meal_short = top_meal[:22] + ('...' if len(top_meal) > 22 else '')

    # New employees this week (first-time orderers)
    all_emails_before = set()
    for o in all_orders:
        if o['sunday_anchor'] != active_week.get('anchor', ''):
            all_emails_before.add(o['employee_email'])
    new_employees_week = len(set(o['employee_email'] for o in week_orders if o['employee_email']) - all_emails_before)

    # Busiest week ever
    busiest_week_label = '—'
    busiest_week_meals = 0
    for w in sorted_weeks:
        if w['meal_count'] > busiest_week_meals:
            busiest_week_meals = w['meal_count']
            busiest_week_label = w['label']

    # Repeat order rate
    emp_order_counts = Counter(o['employee_email'] for o in all_orders if o['employee_email'])
    repeat_employees = sum(1 for c in emp_order_counts.values() if c > 1)
    repeat_rate = round(repeat_employees / len(emp_order_counts) * 100) if emp_order_counts else 0

    # Program growth (% change vs previous week)
    program_growth = 0
    if len(sorted_weeks) >= 2:
        curr = sorted_weeks[0]['meal_count']
        prev = sorted_weeks[1]['meal_count']
        if prev > 0:
            program_growth = round((curr - prev) / prev * 100)

    co_names = {
        c.get('CompanyID', '').upper(): c.get('CompanyName') or c.get('CompanyID', '')
        for c in companies
    }

    orders_json   = _json.dumps([{
        'order_id': o['order_id'], 'company_id': o['company_id'],
        'employee_name': o['employee_name'], 'employee_email': o['employee_email'],
        'delivery_date': o['delivery_date'], 'sunday_anchor': o['sunday_anchor'],
        'emp_total': round(o['emp_total'], 2),
        'co_total':  round(o['co_total'],  2),
        'bd_total':  round(o['bd_total'],  2),
        'meals': [{'dish_name': m['dish_name'], 'tier': m['tier'],
                   'emp_price': round(m['emp_price'], 2),
                   'co_coverage': round(m['co_coverage'], 2),
                   'bd_coverage': round(m['bd_coverage'], 2)} for m in o['meals']],
    } for o in all_orders])
    invoices_json  = _json.dumps(invoices)
    companies_json = _json.dumps(companies)

    return render_template('bd_admin_dashboard.html',
        companies=companies, companies_json=companies_json,
        co_names=co_names, co_stats=co_stats, week_snapshot=week_snapshot,
        all_orders=all_orders, sorted_weeks=sorted_weeks, active_week=active_week,
        invoices=invoices, invoices_json=invoices_json,
        orders_json=orders_json,
        total_companies=total_companies, active_companies_week=active_companies_week,
        total_meals=total_meals, total_co_spend=total_co_spend, total_bd_spend=total_bd_spend,
        total_emp_spend=total_emp_spend, total_unique_employees=total_unique_employees,
        pending_invoices_value=pending_invoices_value, pending_invoices_count=pending_invoices_count,
        paid_invoices_count=sum(1 for inv in invoices if _inv_status(inv) == 'paid'),
        paid_invoices_value=round(sum(float(inv.get('AmountDue') or inv.get('companyOwed') or inv.get('CompanyOwed') or 0) for inv in invoices if _inv_status(inv) == 'paid'), 2),
        week_order_count=week_order_count, week_meal_count=week_meal_count,
        week_employees=week_employees, week_revenue=week_revenue,
        total_revenue=total_revenue, total_orders=total_orders, avg_order_value=avg_order_value,
        top_company=co_names.get(top_company_id, top_company_id),
        top_company_meals=top_company_meals, top_company_revenue=top_company_revenue,
        top_meal=top_meal_short, top_meal_count=top_meal_count, top_meal_revenue=top_meal_revenue,
        new_employees_week=new_employees_week,
        busiest_week=busiest_week_label, busiest_week_meals=busiest_week_meals,
        repeat_rate=repeat_rate, program_growth=program_growth,
    )


@app.route('/bd-admin/invoice-status', methods=['POST'])
@admin_required
def bd_admin_invoice_status():
    invoice_id     = request.form.get('invoice_id', '').strip()
    status         = request.form.get('status', '').strip()
    payment_method = request.form.get('payment_method', '').strip()
    notes          = request.form.get('notes', '').strip()
    result = _gas_post({
        'action': 'update_invoice_status',
        'invoice_id': invoice_id, 'status': status,
        'payment_method': payment_method, 'notes': notes,
    }, timeout=12)
    return jsonify({'success': bool(result and result.get('success'))})


# ─────────────────────────────────────────────────────────────
# BD ADMIN REPORTS
# ─────────────────────────────────────────────────────────────
def _get_week_orders(sunday_anchor):
    """Fetch all corporate orders for a given week, return as list of dicts."""
    raw = _gas_post({'action': 'get_corporate_orders', 'sunday_anchor': sunday_anchor}, timeout=15) or []
    if not isinstance(raw, list):
        raw = []
    return raw


def _get_sorted_weeks():
    """Return list of week anchors for the dropdown."""
    start = _current_monday()
    weeks = []
    for i in range(-4, 6):
        monday = start + timedelta(weeks=i)
        sunday = (monday - timedelta(days=1))
        anchor = sunday.strftime('%Y-%m-%d')
        delivery = monday.strftime('%A, %b %d, %Y')
        weeks.append({'anchor': anchor, 'delivery_label': f'Delivery on: {delivery}'})
    return weeks

def _nice_delivery_date(sunday_anchor):
    """Convert 2026-03-29 to 'Monday, Mar 30, 2026' (delivery = Sunday + 1)."""
    try:
        sun = datetime.strptime(sunday_anchor, '%Y-%m-%d')
        mon = sun + timedelta(days=1)
        return mon.strftime('%A, %b %d, %Y')
    except Exception:
        return sunday_anchor

@app.route('/bd-admin/report/production')
@admin_required
def report_production():
    week = request.args.get('week', '')
    fmt = request.args.get('format', '')
    orders = _get_week_orders(week)

    # Group by dish + diet + SKU
    dishes = {}
    for o in orders:
        name = o.get('DishName', 'Unknown')
        diet = o.get('DietType', 'Unknown')
        sku = str(o.get('MealID', '')).strip()
        key = f"{sku}|{name}|{diet}"
        if key not in dishes:
            dishes[key] = {'name': name, 'diet': diet, 'sku': sku, 'count': 0}
        dishes[key]['count'] += 1

    # Sort by count desc, then diet (Meat first), then name alpha, then SKU
    sorted_dishes = sorted(dishes.values(), key=lambda d: (-d['count'], 0 if 'meat' in d['diet'].lower() else 1, d['name'].lower(), d['sku']))
    total = sum(d['count'] for d in sorted_dishes)

    if fmt == 'csv':
        import io, csv
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Qty', 'Diet', 'Dish Name', 'SKU'])
        for d in sorted_dishes:
            cw.writerow([d['count'], d['diet'], d['name'], d['sku']])
        output = make_response(si.getvalue())
        output.headers['Content-Disposition'] = f'attachment; filename=production_{week}.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    return render_template('report_production.html',
                           week=week, delivery_date=_nice_delivery_date(week),
                           dishes=sorted_dishes, total=total,
                           all_weeks=_get_sorted_weeks())
@app.route('/api/internal/orders')
def api_internal_orders():
    """Culinary-ops sync endpoint — returns aggregated order counts for the week."""
    key = request.headers.get('X-Sync-Key', '') or request.args.get('key', '')
    if key != CULINARY_SYNC_KEY:
        return jsonify({'error': 'unauthorized'}), 401

    week = request.args.get('week', '')
    if not week:
        # Default to current week's sunday anchor
        monday = _current_monday()
        week = (monday - timedelta(days=1)).strftime('%Y-%m-%d')

    orders = _get_week_orders(week)

    # Aggregate: one row per order → group by MealID
    meals = {}
    for o in orders:
        sku = str(o.get('MealID', '')).strip()        # e.g. "#509"
        if not sku:
            continue
        company = o.get('CompanyName', o.get('CompanyID', 'Unknown'))
        diet_raw = o.get('DietType', '').lower()
        diet = 'vegan' if 'vegan' in diet_raw or 'plant' in diet_raw else 'meat'
        if sku not in meals:
            meals[sku] = {
                'meal_id':   sku,
                'dish_name': o.get('DishName', 'Unknown'),
                'diet':      diet,
                'count':     0,
                'by_company': defaultdict(int),
            }
        meals[sku]['count'] += 1
        meals[sku]['by_company'][company] += 1

    result_meals = []
    for m in meals.values():
        result_meals.append({
            'meal_id':    m['meal_id'],
            'dish_name':  m['dish_name'],
            'diet':       m['diet'],
            'count':      m['count'],
            'by_company': [{'company': c, 'count': n} for c, n in m['by_company'].items()],
        })

    companies = list({o.get('CompanyName', o.get('CompanyID', '')) for o in orders if o.get('CompanyName') or o.get('CompanyID')})

    return jsonify({
        'ok':           True,
        'week':         week,
        'total_orders': sum(m['count'] for m in result_meals),
        'companies':    companies,
        'meals':        result_meals,
    })


@app.route('/api/internal/menu', methods=['POST'])
def api_internal_menu():
    """Culinary-ops pushes available meal IDs so employees only see those meals."""
    key = request.headers.get('X-Sync-Key', '') or request.args.get('key', '')
    if key != CULINARY_SYNC_KEY:
        return jsonify({'error': 'unauthorized'}), 401

    data = request.get_json(force=True) or {}
    anchor = data.get('week')
    meal_ids = data.get('meal_ids', [])

    if not anchor or not meal_ids:
        return jsonify({'error': 'week and meal_ids are required'}), 400

    _available_meals[anchor] = meal_ids
    _menu_cache.pop(f'menu_{anchor}', None)  # bust cache so filter applies immediately

    log.info('Menu pushed for week %s: %d meals', anchor, len(meal_ids))
    return jsonify({'ok': True, 'week': anchor, 'meals_published': len(meal_ids)})



@app.route('/bd-admin/report/picklists')
@admin_required
def report_picklists():
    week = request.args.get('week', '')
    orders = _get_week_orders(week)

    # Group by company, then by dish
    companies = {}
    for o in orders:
        co = o.get('CompanyName', o.get('CompanyID', 'Unknown'))
        dish = o.get('DishName', 'Unknown')
        diet = o.get('DietType', '')
        if co not in companies:
            companies[co] = {'dishes': {}, 'total': 0}
        if dish not in companies[co]['dishes']:
            companies[co]['dishes'][dish] = {'count': 0, 'diet': diet}
        companies[co]['dishes'][dish]['count'] += 1
        companies[co]['total'] += 1

    # Add SKU to each dish
    for co in companies.values():
        for dish_name, dish_data in co['dishes'].items():
            # Find SKU from orders
            for o in orders:
                if o.get('DishName') == dish_name:
                    dish_data['sku'] = str(o.get('MealID', '')).strip()
                    break

    return render_template('report_picklists.html',
                           week=week, delivery_date=_nice_delivery_date(week),
                           companies=companies, all_weeks=_get_sorted_weeks())


@app.route('/bd-admin/report/labels')
@admin_required
def report_labels():
    week = request.args.get('week', '')
    fmt = request.args.get('format', '')
    orders = _get_week_orders(week)

    labels = []
    for o in orders:
        last = (o.get('EmployeeName', '') or '').split()[-1] if o.get('EmployeeName') else ''
        labels.append({
            'company': o.get('CompanyName', o.get('CompanyID', '')),
            'name':    o.get('EmployeeName', ''),
            'last':    last,
            'dish':    o.get('DishName', 'Unknown'),
            'diet':    o.get('DietType', ''),
            'sku':     str(o.get('MealID', '')).strip(),
        })

    # Sort: company ASC, then Z→A by last name, then dish ASC
    from itertools import groupby as _groupby
    labels.sort(key=lambda l: (l['company'].lower(), l['dish'].lower()))
    sorted_labels = []
    for co, group in _groupby(sorted(labels, key=lambda l: l['company'].lower()), key=lambda l: l['company']):
        co_labels = list(group)
        co_labels.sort(key=lambda l: (l['last'].lower() if l['last'] else ''), reverse=True)
        sorted_labels.append({'company': co, 'labels': co_labels})

    if fmt == 'csv':
        import io, csv
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Company', 'Employee', 'Dish', 'SKU', 'Diet'])
        for g in sorted_labels:
            for l in g['labels']:
                cw.writerow([g['company'], l['name'], l['dish'], l['sku'], l['diet']])
        output = make_response(si.getvalue())
        output.headers['Content-Disposition'] = f'attachment; filename=labels_{week}.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    return render_template('report_labels.html',
                           week=week, delivery_date=_nice_delivery_date(week),
                           company_groups=sorted_labels, all_weeks=_get_sorted_weeks())


@app.route('/bd-admin/report/delivery')
@admin_required
def report_delivery():
    week = request.args.get('week', '')
    orders = _get_week_orders(week)

    # Group by company for the delivery sheet
    companies_data = {}
    all_companies = {}
    # Get company details from cache
    with _company_cache_lock:
        for cid, entry in _company_cache.items():
            if entry.get('data') and entry['data'].get('company'):
                all_companies[cid.upper()] = entry['data']['company']

    for o in orders:
        cid = str(o.get('CompanyID', '')).upper()
        if cid not in companies_data:
            c = all_companies.get(cid, {})
            companies_data[cid] = {
                'client':    o.get('CompanyName', cid),
                'meals':     0,
                'address':   c.get('AddressLine1', ''),
                'city':      c.get('City', ''),
                'province':  c.get('Province', ''),
                'postal':    c.get('PostalCode', ''),
                'email':     c.get('PrimaryContactEmail', ''),
                'phone':     c.get('PrimaryContactPhone', ''),
                'notes':     c.get('DeliveryInstructions', ''),
                'delivery_day': c.get('DeliveryDay', ''),
            }
        companies_data[cid]['meals'] += 1

    delivery_rows = sorted(companies_data.values(), key=lambda r: r['client'])
    fmt = request.args.get('format', '')

    if fmt == 'csv':
        import io, csv
        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(['Client','Meals','Total','Address','Gate Code','Email','Phone','Notes','Bags','Duration','Business Hours','Assigned Driver'])
        for r in delivery_rows:
            addr = f"{r['address']}, {r['city']}, {r['province']} {r['postal']}".strip(', ')
            cw.writerow([r['client'], r['meals'], '', addr, '', r['email'], r['phone'], r['notes'], '', '', '', ''])
        output = make_response(si.getvalue())
        output.headers['Content-Disposition'] = f'attachment; filename=delivery_stops_{week}.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    return render_template('report_delivery.html',
                           week=week, delivery_date=_nice_delivery_date(week),
                           rows=delivery_rows, all_weeks=_get_sorted_weeks())


# ─────────────────────────────────────────────────────────────
# OFFICE MANAGER PORTAL
# ─────────────────────────────────────────────────────────────
def manager_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('manager_company_id'):
            return redirect(url_for('manager_login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/manager', methods=['GET', 'POST'])
def manager_login():
    error = None
    if request.method == 'POST':
        company_id = request.form.get('company_id', '').strip().upper()
        password   = request.form.get('password', '').strip()
        data = _cached_get_company(company_id)
        if data and data.get('found'):
            company   = data.get('company', {})
            stored_pw = str(company.get('ManagerPassword', '') or '1234')
            if password == stored_pw:
                session['manager_company_id']   = company_id
                session['manager_company_name'] = company.get('CompanyName', company_id)
                return redirect(url_for('manager_dashboard'))
            error = 'Incorrect password.'
        else:
            error = 'Company not found.'
    return render_template('manager_login.html', error=error)


@app.route('/manager/auth')
def manager_auth():
    """Gate screen → dashboard: verify employee manager session token, set Flask session."""
    token = request.args.get('token', '').strip()
    if not token:
        return redirect(url_for('manager_login'))
    result = _gas_post({'action': 'verify_manager_token', 'token': token}, timeout=12)
    if result and result.get('valid'):
        company = result.get('company') or {}
        session['manager_company_id']   = company.get('CompanyID', '')
        session['manager_company_name'] = company.get('CompanyName', '')
        return redirect(url_for('manager_dashboard'))
    return redirect(url_for('manager_login'))


@app.route('/manager/dashboard')
@manager_required
def manager_dashboard():
    import json
    company_id = session.get('manager_company_id')

    # Company data comes from cache (populated at startup) — instant
    company = (_cached_get_company(company_id) or {}).get('company', {})

    # Fire remaining GAS calls in parallel — cuts load from ~15s sequential to ~5s
    results = {}
    def _fetch(key, payload, timeout):
        results[key] = _gas_post(payload, timeout=timeout) or {}

    threads = [
        threading.Thread(target=_fetch, args=('pin',      {'action': 'get_company_pin',      'company_id': company_id}, 8)),
        threading.Thread(target=_fetch, args=('employees',{'action': 'get_employees',        'company_id': company_id}, 10)),
        threading.Thread(target=_fetch, args=('invoices', {'action': 'get_invoices',         'company_id': company_id}, 10)),
        threading.Thread(target=_fetch, args=('orders',   {'action': 'get_corporate_orders', 'company_id': company_id}, 15)),
        threading.Thread(target=_fetch, args=('levels',   {'action': 'get_benefit_levels',   'company_id': company_id}, 8)),
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    current_pin = results.get('pin', {}).get('pin', '')
    employees   = results.get('employees', {}).get('employees', [])
    invoices    = results.get('invoices', {}).get('invoices', [])
    raw = results.get('orders', {})
    if not isinstance(raw, list): raw = []
    if not isinstance(raw, list):
        raw = []

    # ── Group individual meal rows by OrderID ──────────────────
    order_map = {}
    for row in raw:
        oid = str(row.get('OrderID') or '').strip()
        if not oid:
            oid = f"{row.get('EmployeeEmail','anon')}-{row.get('SundayAnchor','')}"
        if oid not in order_map:
            order_map[oid] = {
                'order_id':      oid,
                'employee_name': row.get('EmployeeName', ''),
                'employee_email': row.get('EmployeeEmail', ''),
                'delivery_date': str(row.get('DeliveryDate', '') or ''),
                'sunday_anchor': str(row.get('SundayAnchor', '') or ''),
                'status':        row.get('Status', ''),
                'meals':         [],
                'emp_total':     0.0,
                'co_total':      0.0,
                'bd_total':      0.0,
            }
        rec  = order_map[oid]
        emp  = float(row.get('EmployeePrice')   or 0)
        co   = float(row.get('CompanyCoverage') or 0)
        bd   = float(row.get('BDCoverage')      or 0)
        rec['meals'].append({
            'dish_name':     row.get('DishName', ''),
            'tier':          row.get('Tier', ''),
            'emp_price':     emp,
            'co_coverage':   co,
            'bd_coverage':   bd,
            'total_subsidy': round(co + bd, 2),
        })
        rec['emp_total'] += emp
        rec['co_total']  += co
        rec['bd_total']  += bd

    all_orders = sorted(order_map.values(), key=lambda o: o['delivery_date'], reverse=True)

    # ── Group orders by week ───────────────────────────────────
    week_map = defaultdict(list)
    for o in all_orders:
        anchor = o['sunday_anchor']
        if anchor:
            week_map[anchor].append(o)

    sorted_weeks = []
    for anchor in sorted(week_map.keys(), reverse=True):
        try:
            anchor_dt  = datetime.strptime(anchor, '%Y-%m-%d')
            monday     = anchor_dt + timedelta(days=1)
            nice_label = f"Week of {monday.strftime('%b %d, %Y')}"
        except Exception:
            nice_label = anchor
        wo = week_map[anchor]
        sorted_weeks.append({
            'anchor':      anchor,
            'label':       nice_label,
            'orders':      wo,
            'order_count': len(wo),
            'meal_count':  sum(len(o['meals']) for o in wo),
            'emp_spend':   sum(o['emp_total'] for o in wo),
            'co_spend':    sum(o['co_total'] for o in wo),
            'bd_spend':    sum(o['bd_total'] for o in wo),
        })

    # ── Monthly summaries with per-tier breakdown ─────────────
    def tier_sort_key(name):
        order = {'free': 0, 'tier1': 1, 'tier2': 2, 'tier3': 3, 'full': 4}
        return order.get(str(name).lower().replace(' ', ''), 5)

    month_map = {}
    for o in all_orders:
        date_str = o['delivery_date']
        if len(date_str) < 7:
            continue
        mk = date_str[:7]
        if mk not in month_map:
            month_map[mk] = {'orders': 0, 'meals': 0,
                             'emp_spend': 0.0, 'co_spend': 0.0, 'bd_spend': 0.0,
                             'tiers': {}}
        md = month_map[mk]
        md['orders'] += 1
        for m in o['meals']:
            tier = str(m.get('tier') or 'Full').strip() or 'Full'
            emp  = m['emp_price']
            co   = m['co_coverage']
            bd   = m['bd_coverage']
            md['meals']     += 1
            md['emp_spend'] += emp
            md['co_spend']  += co
            md['bd_spend']  += bd
            if tier not in md['tiers']:
                md['tiers'][tier] = {'meals': 0, 'emp': 0.0, 'co': 0.0, 'bd': 0.0}
            md['tiers'][tier]['meals'] += 1
            md['tiers'][tier]['emp']   += emp
            md['tiers'][tier]['co']    += co
            md['tiers'][tier]['bd']    += bd

    def fmt_month(mk):
        try:    return datetime.strptime(mk, '%Y-%m').strftime('%B %Y')
        except: return mk

    sorted_monthly = []
    for k in sorted(month_map.keys(), reverse=True):
        v = month_map[k]
        tiers_list = sorted([
            {'name': tn, 'meals': ts['meals'],
             'emp': round(ts['emp'], 2), 'co': round(ts['co'], 2), 'bd': round(ts['bd'], 2),
             'total': round(ts['emp'] + ts['co'] + ts['bd'], 2)}
            for tn, ts in v['tiers'].items()
        ], key=lambda t: tier_sort_key(t['name']))
        sorted_monthly.append({
            'key': k, 'label': fmt_month(k),
            'orders':    v['orders'],    'meals':    v['meals'],
            'emp_spend': round(v['emp_spend'], 2),
            'co_spend':  round(v['co_spend'],  2),
            'bd_spend':  round(v['bd_spend'],  2),
            'tiers':     tiers_list,
        })

    # ── Active week = most recent week with orders ────────────
    empty_week  = {'orders': [], 'order_count': 0, 'meal_count': 0,
                   'emp_spend': 0.0, 'co_spend': 0.0, 'bd_spend': 0.0,
                   'label': 'Latest Week', 'anchor': ''}
    active_week = sorted_weeks[0] if sorted_weeks else empty_week

    # ── Staff participation ────────────────────────────────────
    active_week_unique     = len(set(
        o['employee_email'] for o in active_week['orders'] if o.get('employee_email')
    ))
    total_unique_employees = len(set(
        o['employee_email'] for o in all_orders if o.get('employee_email')
    ))
    _denom = total_unique_employees or 1
    active_week_pct = round(active_week_unique / _denom * 100)
    if sorted_weeks and total_unique_employees:
        _week_pcts = [
            len(set(o['employee_email'] for o in w['orders'] if o.get('employee_email'))) / total_unique_employees * 100
            for w in sorted_weeks
        ]
        avg_participation_pct = round(sum(_week_pcts) / len(_week_pcts))
    else:
        avg_participation_pct = 0

    # ── All-time totals ────────────────────────────────────────
    total_meals    = sum(len(o['meals'])  for o in all_orders)
    total_co_spend = sum(o['co_total']   for o in all_orders)
    total_bd_spend = sum(o['bd_total']   for o in all_orders)

    # ── Serialize orders for JS invoice modal ─────────────────
    orders_json = json.dumps([{
        'order_id':      o['order_id'],
        'employee_name': o['employee_name'],
        'employee_email': o.get('employee_email', ''),
        'delivery_date': o['delivery_date'],
        'sunday_anchor': o.get('sunday_anchor', ''),
        'status':        o.get('status', ''),
        'emp_total':     round(o['emp_total'], 2),
        'co_total':      round(o['co_total'],  2),
        'bd_total':      round(o['bd_total'],  2),
        'meals':         [{
            'dish_name':     m['dish_name'],
            'tier':          m['tier'],
            'emp_price':     round(m['emp_price'],    2),
            'total_subsidy': round(m['total_subsidy'], 2),
        } for m in o['meals']],
    } for o in all_orders])

    saved_tab = request.args.get('saved')

    return render_template('manager_dashboard.html',
                           company=company,
                           company_id=company_id,
                           company_name=session.get('manager_company_name'),
                           total_meals=total_meals,
                           total_co_spend=total_co_spend,
                           total_bd_spend=total_bd_spend,
                           active_week=active_week,
                           active_week_unique=active_week_unique,
                           active_week_pct=active_week_pct,
                           avg_participation_pct=avg_participation_pct,
                           total_unique_employees=total_unique_employees,
                           sorted_weeks=sorted_weeks,
                           sorted_monthly=sorted_monthly,
                           orders_json=orders_json,
                           current_pin=current_pin,
                           employees=employees,
                           invoices=invoices,
                           benefit_levels=results.get('levels', {}).get('levels', []),
                           saved_tab=saved_tab)


@app.route('/manager/update-account', methods=['POST'])
@manager_required
def manager_update_account():
    company_id = session.get('manager_company_id')
    allowed = ['AddressLine1', 'City', 'PostalCode', 'DeliveryInstructions',
               'PrimaryContactName', 'PrimaryContactEmail', 'PrimaryContactPhone',
               'BillingContactEmail']
    fields = {'action': 'save_company', 'CompanyID': company_id}
    for f in allowed:
        fields[f] = request.form.get(f, '')
    result = _gas_post(fields, timeout=12)
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({'success': bool(result and result.get('success'))})
    return redirect(url_for('manager_dashboard') + '?saved=account')


@app.route('/manager/save-meal-allowances', methods=['POST'])
@manager_required
def manager_save_meal_allowances():
    """Save meal count changes from manager portal. Logs all changes."""
    company_id = session.get('manager_company_id')
    manager_email = session.get('manager_email', 'manager')
    data = request.get_json(force=True)
    changes = data.get('changes', [])
    if not changes:
        return jsonify({'success': False, 'error': 'No changes provided'})
    # Check permission
    comp = _cached_get_company(company_id)
    if not comp or not comp.get('found'):
        return jsonify({'success': False, 'error': 'Company not found'})
    company = comp.get('company', {})
    if str(company.get('ManagerCanEditMeals', '')).upper() != 'TRUE':
        return jsonify({'success': False, 'error': 'Editing not enabled for this company'})
    # Send to GAS
    result = _gas_post({
        'action': 'manager_save_meal_allowances',
        'company_id': company_id,
        'changes': changes,
        'changed_by': manager_email
    }, timeout=15)
    # Clear company cache so changes take effect immediately
    with _company_cache_lock:
        _company_cache.pop(company_id.upper(), None)
    return jsonify(result or {'success': False, 'error': 'GAS timeout'})


@app.route('/manager/meal-change-log')
@manager_required
def manager_meal_change_log():
    """Fetch the meal allowance change log for this company."""
    company_id = session.get('manager_company_id')
    result = _gas_post({
        'action': 'get_meal_change_log',
        'company_id': company_id
    }, timeout=10)
    return jsonify(result or {'log': []})


@app.route('/manager/par-levels', methods=['GET'])
@manager_required
def get_par_levels():
    """Load par level data for this company."""
    company_id = session.get('manager_company_id')
    result = _gas_post({
        'action': 'get_par_levels',
        'company_id': company_id
    }, timeout=10)
    return jsonify(result or {'levels': {}})


@app.route('/manager/par-levels', methods=['POST'])
@manager_required
def save_par_levels():
    """Save par level quantities/status for this company."""
    company_id = session.get('manager_company_id')
    manager_email = session.get('manager_email', 'manager')
    data = request.get_json(force=True)
    result = _gas_post({
        'action': 'save_par_levels',
        'company_id': company_id,
        'levels': data.get('levels', {}),
        'override': data.get('override', False),
        'changed_by': manager_email
    }, timeout=12)
    return jsonify(result or {'success': False, 'error': 'GAS timeout'})


@app.route('/manager/par-levels/confirm', methods=['POST'])
@manager_required
def confirm_par_order():
    """Confirm the weekly par level order."""
    company_id = session.get('manager_company_id')
    manager_email = session.get('manager_email', 'manager')
    data = request.get_json(force=True)
    result = _gas_post({
        'action': 'confirm_par_order',
        'company_id': company_id,
        'levels': data.get('levels', {}),
        'changed_by': manager_email
    }, timeout=15)
    return jsonify(result or {'success': False, 'error': 'GAS timeout'})


# Par level catalog cache (items from 9.0 sheet)
_par_catalog_cache = {'data': None, 'ts': 0}

@app.route('/manager/par-catalog')
@manager_required
def get_par_catalog():
    """Get all available items grouped by par level category. Cached 30 min."""
    now = time.time()
    if _par_catalog_cache['data'] and (now - _par_catalog_cache['ts'] < 1800):
        return jsonify(_par_catalog_cache['data'])
    result = _gas_post({'action': 'get_par_catalog'}, timeout=20)
    if result and result.get('catalog'):
        _par_catalog_cache['data'] = result
        _par_catalog_cache['ts'] = now
    return jsonify(result or {'catalog': {}})


@app.route('/manager/invoice-status', methods=['POST'])
@manager_required
def manager_invoice_status():
    """Admin/manager endpoint to update invoice status."""
    body          = request.get_json(force=True) or {}
    invoice_id    = body.get('invoice_id', '').strip()
    status        = body.get('status', '').strip()
    payment_method = body.get('payment_method', '').strip()
    notes         = body.get('notes', '').strip()
    if not invoice_id or status not in ('pending', 'sent', 'paid'):
        return jsonify({'success': False, 'error': 'Invalid params'}), 400
    result = _gas_post({
        'action': 'update_invoice_status',
        'invoice_id': invoice_id, 'status': status,
        'payment_method': payment_method, 'notes': notes
    }, timeout=12)
    return jsonify(result or {'success': False})


@app.route('/manager/remove-employee', methods=['POST'])
@manager_required
def manager_remove_employee():
    company_id = session.get('manager_company_id')
    email = request.get_json(force=True).get('email', '').strip().lower()
    if not email:
        return jsonify({'success': False, 'error': 'Missing email'}), 400
    result = _gas_post({'action': 'remove_employee', 'company_id': company_id, 'email': email}, timeout=12)
    return jsonify(result or {'success': False, 'error': 'GAS error'})


@app.route('/manager/resend-link', methods=['POST'])
@manager_required
def manager_resend_link():
    company_id = session.get('manager_company_id')
    email = request.get_json(force=True).get('email', '').strip().lower()
    if not email:
        return jsonify({'success': False, 'error': 'Missing email'}), 400
    token      = secrets.token_hex(32)
    sign_in_url = f"{APP_BASE_URL}/work?token={token}&co={company_id}"
    _store_magic_token(token, email, company_id)
    result = _gas_post({
        'action': 'create_magic_token',
        'email': email, 'company_id': company_id,
        'token_override': token, 'sign_in_url': sign_in_url
    }, timeout=15)
    return jsonify({'success': bool(result and result.get('success'))})


@app.route('/manager/logout')
def manager_logout():
    session.pop('manager_company_id', None)
    session.pop('manager_company_name', None)
    return redirect(url_for('manager_login'))


@app.route('/bd-admin/manager-view/<company_id>')
def admin_manager_view(company_id):
    """Let admin preview a company's manager dashboard by setting the session."""
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))
    company_id = company_id.strip().upper()
    result = _cached_get_company(company_id)
    if result and result.get('found'):
        c = result['company']
        session['manager_company_id'] = company_id
        session['manager_company_name'] = c.get('CompanyName', company_id)
    return redirect(url_for('manager_dashboard'))


@app.route('/bd-admin/send-reminders', methods=['POST'])
def admin_send_reminders():
    """Send order reminder emails to employees who haven't ordered this week."""
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(force=True) or {}
    payload = {'action': 'send_order_reminders'}
    if data.get('company_ids'):
        payload['company_ids'] = data['company_ids']
    result = _gas_post(payload)
    if result:
        return jsonify(result)
    return jsonify({'error': 'Failed to send reminders'}), 502


@app.route('/bd-admin/credit-notes', methods=['GET', 'POST'])
def admin_credit_notes():
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    if request.method == 'POST':
        data = request.get_json(force=True) or {}
        action = data.get('_action', 'create_credit_note')
        payload = {'action': action}
        payload.update(data)
        del payload['_action']
        result = _gas_post(payload)
        return jsonify(result) if result else jsonify({'error': 'Failed'}), 502
    # GET — list all
    result = _gas_post({'action': 'get_credit_notes'})
    return jsonify(result) if result else jsonify({'creditNotes': []})


@app.route('/bd-admin/ar-summary')
def admin_ar_summary():
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    result = _gas_post({'action': 'get_ar_summary'})
    return jsonify(result) if result else jsonify({'error': 'Failed to load AR data'}), 502


# ─────────────────────────────────────────────────────────────
# BETTERDAY FOR WORK — CORPORATE EMPLOYEE ORDERING
# ─────────────────────────────────────────────────────────────
@app.route('/lander')
def lander_redirect():
    """Instant pass-through — redirects to /work immediately, token verified client-side."""
    import json as _json
    qs = request.query_string.decode('utf-8')
    target = '/work' + ('?' + qs if qs else '')
    html = f'''<!DOCTYPE html><html><head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="0;url={target}">
<script>window.location.replace({_json.dumps(target)});</script>
</head><body></body></html>'''
    return html, 200, {'Cache-Control': 'no-store, no-cache', 'Content-Type': 'text/html'}


@app.route('/api/magic-session')
def magic_session():
    """Consume the one-shot server-side magic link session set by /lander."""
    emp     = session.pop('magic_employee', None)
    company = session.pop('magic_company', None)
    if emp and company:
        return jsonify({'valid': True, 'employee': emp, 'company': company})
    return jsonify({'valid': False})


@app.route('/work')
def work_order():
    """Employee-facing corporate ordering portal."""
    return render_template('work.html')


@app.route('/api/companies')
def companies_list():
    """Return all companies currently in the Flask cache — instant, no GAS call."""
    with _company_cache_lock:
        companies = [
            entry['data']['company']
            for entry in _company_cache.values()
            if entry['data'].get('found') and entry['data'].get('company')
        ]
    return jsonify({'companies': companies})


@app.route('/api/company/<company_id>')
def company_lookup(company_id):
    """Fast cached company lookup — avoids GAS cold-start on every user keystroke."""
    result = _cached_get_company(company_id)
    if result is None:
        return jsonify({'error': 'lookup failed'}), 502
    return jsonify(result)


@app.route('/api/helcim/checkout', methods=['POST'])
def helcim_checkout():
    """Initialize a HelcimPay.js checkout session for employee card payment."""
    if not HELCIM_API_TOKEN:
        return jsonify({'error': 'Payment processing not configured'}), 503
    data = request.get_json(force=True) or {}
    amount = float(data.get('amount', 0))
    if amount <= 0:
        return jsonify({'error': 'Invalid amount'}), 400
    try:
        resp = requests.post(
            'https://api.helcim.com/v2/helcim-pay/initialize',
            headers={
                'accept': 'application/json',
                'api-token': HELCIM_API_TOKEN,
                'content-type': 'application/json'
            },
            json={
                'paymentType': 'purchase',
                'amount': round(amount, 2),
                'currency': 'CAD',
            },
            timeout=15
        )
        if resp.status_code == 200:
            result = resp.json()
            # Store secretToken server-side for validation later
            checkout_token = result.get('checkoutToken', '')
            secret_token = result.get('secretToken', '')
            if checkout_token:
                with _token_store_lock:
                    _token_store['helcim_' + checkout_token] = {
                        'secret': secret_token,
                        'amount': amount,
                        'order_id': data.get('order_id', ''),
                        'created_at': time.time()
                    }
            return jsonify({'checkoutToken': checkout_token})
        else:
            log.warning('Helcim init failed: status=%s body=%s', resp.status_code, resp.text[:500])
            return jsonify({'error': 'Payment initialization failed', 'detail': resp.text[:200]}), 502
    except Exception as e:
        log.error('Helcim init error: %s', e)
        return jsonify({'error': 'Payment service unavailable', 'detail': str(e)}), 503


@app.route('/api/helcim/test')
def helcim_test():
    """Quick test to verify Helcim API connectivity. Remove before go-live."""
    if not HELCIM_API_TOKEN:
        return jsonify({'ok': False, 'error': 'HELCIM_API_TOKEN not set', 'token_len': 0})
    try:
        resp = requests.post(
            'https://api.helcim.com/v2/helcim-pay/initialize',
            headers={'accept': 'application/json', 'api-token': HELCIM_API_TOKEN, 'content-type': 'application/json'},
            json={'paymentType': 'purchase', 'amount': 1.00, 'currency': 'CAD'},
            timeout=15
        )
        return jsonify({'ok': resp.status_code == 200, 'status': resp.status_code, 'body': resp.text[:500], 'token_preview': HELCIM_API_TOKEN[:8] + '...'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/work/submit', methods=['POST'])
def work_submit():
    """Server-side order submission endpoint."""
    data   = request.get_json(force=True) or {}
    result = _gas_post({
        'action':           'submit_corporate_order',
        'company_id':       data.get('company_id'),
        'company_name':     data.get('company_name'),
        'delivery_date':    data.get('delivery_date'),
        'sunday_anchor':    data.get('sunday_anchor'),
        'employee_name':    data.get('employee_name'),
        'meal_id':          data.get('meal_id'),
        'dish_name':        data.get('dish_name'),
        'diet_type':        data.get('diet_type'),
        'tier':             data.get('tier'),
        'employee_price':   data.get('employee_price'),
        'company_coverage': data.get('company_coverage'),
        'bd_coverage':      data.get('bd_coverage', '0.00'),
    }, timeout=12)
    if result is None:
        return jsonify({'status': 'error', 'message': 'Submission failed — please try again.'}), 500
    return jsonify({'status': 'ok'}), 200


@app.route('/work/companies')
@admin_required
def work_companies():
    return redirect(url_for('bd_admin_dashboard'))


@app.route('/work/company/<company_id>', methods=['GET', 'POST'])
@admin_required
def company_editor(company_id):
    error   = None
    success = None

    if request.method == 'POST':
        fields = dict(request.form)
        fields['action'] = 'save_company'
        result = _gas_post(fields, timeout=12)
        if result and result.get('success'):
            success = 'Company saved successfully.'
        else:
            error = (result.get('error') if result else None) or 'Save failed — check Apps Script logs.'

    company = {}
    data = _gas_post({'action': 'get_company', 'company_id': company_id}, timeout=10)
    if data:
        company = data.get('company', {})

    return render_template('company_editor.html',
                           company=company, company_id=company_id,
                           error=error, success=success)


@app.route('/work/invoices/<sunday>')
@admin_required
def corporate_invoices(sunday):
    all_corp       = _gas_post({'action': 'get_corporate_orders'}, timeout=15) or []
    companies_list = _gas_post({'action': 'get_all_companies'}, timeout=10) or []

    company_map = {}
    if isinstance(companies_list, list):
        for c in companies_list:
            if isinstance(c, dict):
                company_map[c.get('CompanyID', '')] = c

    week_orders = [o for o in (all_corp if isinstance(all_corp, list) else [])
                   if isinstance(o, dict) and o.get('SundayAnchor') == sunday]

    by_company = defaultdict(list)
    for o in week_orders:
        by_company[o.get('CompanyID', '—')].append(o)

    invoices = []
    for cid, orders in by_company.items():
        c_info = company_map.get(cid, {})
        fp     = float(c_info.get('BasePrice') or c_info.get('FullPrice') or 16.99)

        tier_summary = defaultdict(lambda: {'count': 0, 'emp_total': 0.0,
                                            'co_total': 0.0, 'bd_total': 0.0})
        employees = defaultdict(list)
        for o in orders:
            tier = (o.get('Tier') or 'full').lower()
            ep   = float(o.get('EmployeePrice') or 0)
            cc   = float(o.get('CompanyCoverage') or 0)
            bd   = float(o.get('BDCoverage') or 0)
            tier_summary[tier]['count']     += 1
            tier_summary[tier]['emp_total'] += ep
            tier_summary[tier]['co_total']  += cc
            tier_summary[tier]['bd_total']  += bd
            employees[o.get('EmployeeName', '—')].append(o)

        invoices.append({
            'company_id':   cid,
            'company_name': orders[0].get('CompanyName', cid),
            'company_info': c_info,
            'orders':       orders,
            'employees':    dict(employees),
            'tier_summary': dict(tier_summary),
            'grand_emp':    sum(float(o.get('EmployeePrice') or 0) for o in orders),
            'grand_co':     sum(float(o.get('CompanyCoverage') or 0) for o in orders),
            'grand_bd':     sum(float(o.get('BDCoverage') or 0) for o in orders),
            'grand_retail': len(orders) * fp,
            'meal_count':   len(orders),
            'full_price':   fp,
        })

    return render_template('corporate_invoices.html',
                           invoices=invoices,
                           sunday=format_week_header(sunday),
                           sunday_raw=sunday)


@app.route('/work/admin')
@admin_required
def work_admin():
    return redirect(url_for('bd_admin_dashboard'))


# ─────────────────────────────────────────────────────────────
# MENU BUILDER
# ─────────────────────────────────────────────────────────────
@app.route('/menubuilder')
@admin_required
def menubuilder():
    return render_template('menubuilder-chef.html')


# ─────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    threading.Thread(target=_warmup_gas, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
