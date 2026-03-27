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

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
GOOGLE_SCRIPT_URL = os.environ.get(
    'GOOGLE_SCRIPT_URL',
    "https://script.google.com/macros/s/AKfycbxKVyW7sguwUq3TYsk-xtIF2fLicefaxTwl_PHjQVjt5-OiBarPQ_nXb_0H927NXAMG0w/exec"
)
APP_BASE_URL    = os.environ.get('APP_BASE_URL', 'https://betterday-app.onrender.com')
ADMIN_PASSWORD  = os.environ.get('ADMIN_PASSWORD',   'betterday2024')
app.secret_key  = os.environ.get('FLASK_SECRET_KEY', 'bd-dev-secret-change-in-prod')

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# MAGIC LINK TOKEN STORE  (Flask-side, bypasses GAS verify_magic_token)
# ─────────────────────────────────────────────────────────────
_token_store      = {}   # token → {email, company_id, created_at, used}
_token_store_lock = threading.Lock()
_TOKEN_TTL        = 900  # 15 minutes

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
            next_url = request.args.get('next') or url_for('work_admin')
            return redirect(next_url)
        error = 'Incorrect password.'
    return render_template('admin_login.html', error=error)


@app.route('/admin-logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))


# ─────────────────────────────────────────────────────────────
# GAS PROXY  — single endpoint, keeps the Apps Script URL
#              server-side and out of the browser
# ─────────────────────────────────────────────────────────────
@app.route('/api/gas', methods=['POST'])
def gas_proxy():
    payload = request.get_json(force=True) or {}

    # ── create_magic_token: Flask generates + stores token; GAS just sends the email ──
    if payload.get('action') == 'create_magic_token':
        token      = secrets.token_hex(32)
        company_id = str(payload.get('company_id', '')).strip().upper()
        email      = str(payload.get('email', '')).strip().lower()
        _store_magic_token(token, email, company_id)
        payload['token_override'] = token
        payload['sign_in_url'] = f"{APP_BASE_URL}/work?token={token}&co={company_id}"

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
def _gas_get(params, timeout=10):
    try:
        r = requests.get(GOOGLE_SCRIPT_URL, params=params, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception as ex:
        log.warning('GAS GET error (%s): %s', params, ex)
        return None


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


@app.template_filter('decode_school')
def decode_school_filter(s):
    return str(s).replace('+', ' ')


# ─────────────────────────────────────────────────────────────
# SCHOOL BOOKING — PUBLIC CALENDAR
# ─────────────────────────────────────────────────────────────
@app.route('/')
def index():
    booked_date_raw  = request.cookies.get('user_booked_date')
    booked_date_nice = get_nice_date(booked_date_raw) if booked_date_raw else None

    taken_dates = []
    taken_raw = _gas_get({'action': 'get_bookings'}) or []
    if isinstance(taken_raw, list):
        for row in taken_raw:
            if isinstance(row, list) and row and 'Date' not in str(row[0]):
                taken_dates.append(str(row[0]).split('T')[0])

    blocked_dates  = _gas_post({'action': 'get_blocked_dates'}) or []
    all_unavailable = set(taken_dates + (blocked_dates if isinstance(blocked_dates, list) else []))

    start_date = _current_monday()
    today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    weeks = []
    for i in range(10):
        monday = start_date + timedelta(weeks=i)
        days = []
        for d in [monday, monday + timedelta(1), monday + timedelta(2)]:
            d_str = d.strftime('%Y-%m-%d')
            days.append({
                'raw_date': d_str,
                'display':  d.strftime('%A, %b %d'),
                'blocked':  d_str in all_unavailable,
                'past':     d < today_date,
            })
        weeks.append({'week_label': monday.strftime('Week of %b %d'), 'days': days})

    return render_template('index.html', weeks=weeks, booked_date=booked_date_nice)


@app.route('/book/<date_raw>', methods=['GET', 'POST'])
def book(date_raw):
    if request.method == 'GET':
        return render_template('form.html', date_display=date_raw, raw_date=date_raw)

    _gas_post({
        'action':         'book_principal',
        'date':           date_raw,
        'contact_name':   request.form.get('contact_name'),
        'email':          request.form.get('email'),
        'school_name':    request.form.get('school_name'),
        'address':        request.form.get('address'),
        'staff_count':    request.form.get('staff_count'),
        'lunch_time':     request.form.get('lunch_time'),
        'delivery_notes': request.form.get('delivery_notes'),
    })

    resp = make_response(redirect(url_for('index')))
    resp.set_cookie('user_booked_date', date_raw, max_age=60 * 60 * 24 * 30)
    return resp


# ─────────────────────────────────────────────────────────────
# ADMIN DASHBOARD
# ─────────────────────────────────────────────────────────────
@app.route('/BD-Admin')
@admin_required
def bd_admin():
    bookings_raw  = _gas_get({'action': 'get_bookings'}) or []
    all_orders    = _gas_post({'action': 'get_all_orders'}) or []
    blocked_dates = _gas_post({'action': 'get_blocked_dates'}) or []

    order_counts = {}
    if isinstance(all_orders, list):
        for o in all_orders:
            if isinstance(o, dict):
                key = f"{o.get('school')}_{o.get('date')}"
                order_counts[key] = order_counts.get(key, 0) + 1

    production_weeks = {}
    if isinstance(bookings_raw, list):
        for b in bookings_raw:
            try:
                if not isinstance(b, list) or len(b) < 3:
                    continue
                if 'Date' in str(b[0]) or str(b[2]).isdigit():
                    continue

                d_date  = str(b[0]).split('T')[0]
                anchor  = get_sunday_anchor(d_date)
                if not anchor:
                    continue

                deadline_obj  = get_deadline_obj(d_date)
                school_name   = str(b[2])
                is_office     = 'Health' in school_name or 'Headversity' in school_name
                meals_ordered = order_counts.get(f'{school_name}_{d_date}', 0)

                booking_obj = {
                    'delivery_date_raw':     d_date,
                    'delivery_date_display': get_nice_date(d_date),
                    'school':        school_name,
                    'status':        str(b[7]) if len(b) > 7 else 'New Booking',
                    'staff_count':   str(b[4]) if len(b) > 4 else '0',
                    'meals_ordered': meals_ordered,
                    'deadline':      deadline_obj.strftime('%b %d') if deadline_obj else 'TBD',
                    'type':          'Office' if is_office else 'School',
                }

                if anchor not in production_weeks:
                    production_weeks[anchor] = {
                        'nice_date': format_week_header(anchor),
                        'anchor_id': anchor,
                        'bookings':  [],
                    }
                production_weeks[anchor]['bookings'].append(booking_obj)
            except Exception as ex:
                log.debug('bd_admin booking parse error: %s', ex)

    sorted_weeks = dict(sorted(production_weeks.items()))
    start_date   = _current_monday()
    today_date   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    toggle_weeks = []
    for i in range(10):
        monday = start_date + timedelta(weeks=i)
        days = []
        for d in [monday, monday + timedelta(1), monday + timedelta(2)]:
            d_str = d.strftime('%Y-%m-%d')
            days.append({
                'raw_date': d_str,
                'display':  d.strftime('%a, %b %d'),
                'blocked':  d_str in (blocked_dates if isinstance(blocked_dates, list) else []),
                'past':     d < today_date,
            })
        toggle_weeks.append({'week_label': monday.strftime('Week of %b %d'), 'days': days})

    return render_template('admin.html', weeks=sorted_weeks, toggle_weeks=toggle_weeks)


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
    total_unique_employees = len(set(o['employee_email'] for o in all_orders if o['employee_email']))
    total_companies        = len(companies)
    active_companies_week  = len(set(o['company_id'] for o in active_week['orders']))
    pending_invoices_value = round(sum(
        float(inv.get('companyOwed', 0) or 0)
        for inv in invoices if (inv.get('status') or 'pending') == 'pending'
    ), 2)

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
        total_unique_employees=total_unique_employees,
        pending_invoices_value=pending_invoices_value,
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


@app.route('/toggle-date', methods=['POST'])
@admin_required
def toggle_date():
    _gas_post({'action': 'toggle_block_date', 'date': request.form.get('date')})
    return 'OK', 200


# ─────────────────────────────────────────────────────────────
# CULINARY & INVOICES
# ─────────────────────────────────────────────────────────────
@app.route('/culinary-summary/<sunday>')
@admin_required
def culinary_summary(sunday):
    all_orders  = _gas_post({'action': 'get_all_orders'}, timeout=20) or []
    totals      = {'Meat': {}, 'Plant-Based': {}}
    total_count = 0

    if isinstance(all_orders, list):
        for o in all_orders:
            if not isinstance(o, dict):
                continue
            if get_sunday_anchor(o.get('date')) != sunday:
                continue
            dish = str(o.get('dish_name') or f"Dish #{o.get('meal_id')}")
            diet = str(o.get('diet') or '')
            cat  = 'Plant-Based' if ('Plant' in diet or 'Vegan' in diet) else 'Meat'
            totals[cat][dish] = totals[cat].get(dish, 0) + 1
            total_count += 1

    for cat in totals:
        totals[cat] = dict(sorted(totals[cat].items()))

    return render_template('culinary_picklist.html',
                           sunday=format_week_header(sunday),
                           totals=totals, total_count=total_count)


@app.route('/batch-invoices/<sunday>')
@admin_required
def batch_invoices(sunday):
    bookings_raw = _gas_get({'action': 'get_bookings'}) or []
    all_orders   = _gas_post({'action': 'get_all_orders'}) or []

    schools_this_week = {}
    if isinstance(bookings_raw, list):
        for b in bookings_raw:
            try:
                if not isinstance(b, list) or len(b) < 3:
                    continue
                if 'Date' in str(b[0]) or str(b[2]).isdigit():
                    continue
                d_date = str(b[0]).split('T')[0]
                if get_sunday_anchor(d_date) == sunday:
                    schools_this_week[str(b[2])] = get_nice_date(d_date)
            except Exception as ex:
                log.debug('batch_invoices parse: %s', ex)

    summaries = {name: {'delivery_date': nd, 'dishes': {}, 'total': 0}
                 for name, nd in schools_this_week.items()}

    if isinstance(all_orders, list):
        for o in all_orders:
            if not isinstance(o, dict):
                continue
            school = o.get('school')
            if get_sunday_anchor(o.get('date')) == sunday and school in summaries:
                dish = str(o.get('dish_name') or f"Dish #{o.get('meal_id')}")
                summaries[school]['dishes'][dish] = summaries[school]['dishes'].get(dish, 0) + 1
                summaries[school]['total'] += 1

    return render_template('batch_invoices.html',
                           sunday=format_week_header(sunday), summaries=summaries)


# ─────────────────────────────────────────────────────────────
# SCHOOL PROFILE & ORDER MANAGEMENT
# ─────────────────────────────────────────────────────────────
@app.route('/school-profile/<school_name>/<date>')
@admin_required
def school_profile(school_name, date):
    clean = school_name.replace('+', ' ')
    data  = _gas_post({'action': 'get_profile_data', 'school': clean, 'date': date},
                      timeout=12) or {}

    deadline_obj   = get_deadline_obj(date)
    days_left      = (deadline_obj - datetime.now()).days if deadline_obj else -1
    deadline_str   = deadline_obj.strftime('%b %d @ 4:00 PM') if deadline_obj else 'TBD'
    countdown_text = ('⚠️ Orders Closed' if days_left < 0
                      else '🚨 Ends Today!' if days_left == 0
                      else f'⏰ {days_left} Days Left')

    return render_template('profile.html',
                           school=clean, date=date,
                           display_date=get_nice_date(date),
                           deadline=deadline_str, countdown=countdown_text,
                           staff=int(data.get('staff_count', 0) if data else 0),
                           orders=len(data.get('orders', []) if data else []),
                           info=data or {})


@app.route('/update-booking', methods=['POST'])
@admin_required
def update_booking():
    school = request.form.get('school')
    date   = request.form.get('date')
    _gas_post({
        'action': 'update_booking',
        'school': school, 'date': date,
        'status': request.form.get('status'),
        'email':  request.form.get('email'),
    })
    return redirect(url_for('school_profile',
                            school_name=school.replace(' ', '+'), date=date))


@app.route('/download-csv/<school_name>/<date>')
@admin_required
def download_csv(school_name, date):
    clean  = school_name.replace('+', ' ')
    data   = _gas_post({'action': 'get_profile_data', 'school': clean, 'date': date}) or {}
    orders = data.get('orders', []) if isinstance(data, dict) else []

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['Teacher Name', 'Diet', 'Dish ID', 'Dish Name'])
    for o in orders:
        if isinstance(o, dict):
            cw.writerow([o.get('teacher', ''), o.get('diet', ''),
                         str(o.get('meal_id', '')).strip(),
                         o.get('dish_name', 'Unknown Dish')])

    output = make_response(si.getvalue())
    output.headers['Content-Disposition'] = f'attachment; filename={clean}_orders.csv'
    output.headers['Content-type'] = 'text/csv'
    return output


@app.route('/picklist/<school_name>/<date>')
@admin_required
def picklist_print(school_name, date):
    clean  = school_name.replace('+', ' ')
    data   = _gas_post({'action': 'get_profile_data', 'school': clean, 'date': date}) or {}
    orders = data.get('orders', []) if isinstance(data, dict) else []

    summary = {}
    for o in orders:
        if not isinstance(o, dict):
            continue
        mid  = str(o.get('meal_id', '')).strip()
        name = o.get('dish_name') or f'Dish #{mid}'
        if name not in summary:
            summary[name] = {'id': mid, 'count': 0}
        summary[name]['count'] += 1

    return render_template('picklist.html',
                           school=clean, date=date, summary=summary, total=len(orders))


# ─────────────────────────────────────────────────────────────
# TEACHER ORDERING
# ─────────────────────────────────────────────────────────────
@app.route('/order/<delivery_date>')
def teacher_order(delivery_date):
    if request.cookies.get(f'ordered_{delivery_date}'):
        school     = request.args.get('school', 'BetterDay School')
        share_link = url_for('teacher_order', delivery_date=delivery_date,
                             school=school, _external=True)
        return render_template('order_success.html', share_link=share_link, existing=True)

    school       = request.args.get('school', 'BetterDay School')
    anchor       = get_sunday_anchor(delivery_date)
    deadline_obj = get_deadline_obj(delivery_date)
    deadline_str = deadline_obj.strftime('%b %d @ 4:00 PM') if deadline_obj else 'TBD'

    menu_data  = _gas_post({'action': 'get_menu', 'sunday_anchor': anchor}, timeout=15) or {}
    meat_menu  = menu_data.get('meat',  []) if isinstance(menu_data, dict) else []
    vegan_menu = menu_data.get('vegan', []) if isinstance(menu_data, dict) else []
    menu_error = ('Menu is currently unavailable — please try again in a few minutes.'
                  if not meat_menu and not vegan_menu else None)

    return render_template('orderform.html',
                           delivery_date=delivery_date, deadline=deadline_str,
                           meat_menu=meat_menu, vegan_menu=vegan_menu,
                           school_name=school, menu_error=menu_error)


@app.route('/submit-order', methods=['POST'])
def submit_order():
    school = request.form.get('school_name')
    date   = request.form.get('delivery_date')
    _gas_post({
        'action':        'submit_teacher_order',
        'name':          request.form.get('teacher_name'),
        'meal_id':       request.form.get('meal_id'),
        'dish_name':     request.form.get('dish_name'),
        'diet':          request.form.get('dish_diet'),
        'delivery_date': date,
        'school':        school,
        'timestamp':     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }, timeout=10)

    share_link = url_for('teacher_order', delivery_date=date, school=school, _external=True)
    resp = make_response(render_template('order_success.html', share_link=share_link, existing=False))
    resp.set_cookie(f'ordered_{date}', 'true', max_age=60 * 60 * 24 * 30)
    return resp


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


@app.route('/work/submit', methods=['POST'])
def work_submit():
    """Server-side fallback / future Stripe integration point."""
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
    return redirect(url_for('work_admin'))


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
    company_id    = request.args.get('company_id', '')
    sunday_anchor = request.args.get('sunday', '')

    payload = {'action': 'get_corporate_orders'}
    if company_id:    payload['company_id']    = company_id
    if sunday_anchor: payload['sunday_anchor'] = sunday_anchor
    orders = _gas_post(payload) or []

    companies_list     = _gas_post({'action': 'get_all_companies'}) or []
    all_teacher_orders = _gas_post({'action': 'get_all_orders'}) or []
    bookings_raw       = _gas_get({'action': 'get_bookings'}) or []

    # Full corp list always needed for week summaries even when filtered
    full_corp = orders
    if company_id or sunday_anchor:
        full_corp = _gas_post({'action': 'get_corporate_orders'}) or []

    corp_by_anchor = defaultdict(list)
    for o in (full_corp if isinstance(full_corp, list) else []):
        corp_by_anchor[o.get('SundayAnchor', '')].append(o)

    teacher_by_anchor = defaultdict(list)
    for o in (all_teacher_orders if isinstance(all_teacher_orders, list) else []):
        anchor = get_sunday_anchor(o.get('date', ''))
        if anchor:
            teacher_by_anchor[anchor].append(o)

    school_by_anchor = defaultdict(set)
    for b in (bookings_raw if isinstance(bookings_raw, list) else []):
        try:
            if not isinstance(b, list) or len(b) < 3:
                continue
            if 'Date' in str(b[0]) or str(b[2]).isdigit():
                continue
            d_date = str(b[0]).split('T')[0]
            anchor = get_sunday_anchor(d_date)
            if anchor:
                school_by_anchor[anchor].add(str(b[2]))
        except Exception as ex:
            log.debug('work_admin school parse: %s', ex)

    start_date     = _current_monday()
    week_summaries = []
    for i in range(6):
        monday = start_date + timedelta(weeks=i)
        sunday = (monday - timedelta(days=1)).strftime('%Y-%m-%d')

        corp_orders  = corp_by_anchor.get(sunday, [])
        teach_orders = teacher_by_anchor.get(sunday, [])
        school_names = school_by_anchor.get(sunday, set())

        week_summaries.append({
            'anchor':          sunday,
            'nice_date':       format_week_header(sunday),
            'delivery_monday': monday.strftime('%b %d'),
            'offices':         len({o.get('CompanyName') for o in corp_orders if o.get('CompanyName')}),
            'office_meals':    len(corp_orders),
            'employees':       len({o.get('EmployeeName') for o in corp_orders if o.get('EmployeeName')}),
            'schools':         len(school_names),
            'school_meals':    len(teach_orders),
            'total_meals':     len(corp_orders) + len(teach_orders),
        })

    grouped = defaultdict(lambda: defaultdict(list))
    for o in (orders if isinstance(orders, list) else []):
        if isinstance(o, dict):
            grouped[o.get('CompanyName', '—')][o.get('DeliveryDate', '—')].append(o)

    grouped_plain  = {co: dict(wks) for co, wks in grouped.items()}
    company_totals = {co: sum(len(v) for v in wks.values())
                     for co, wks in grouped_plain.items()}

    return render_template('work_admin.html',
                           grouped=grouped_plain,
                           company_totals=company_totals,
                           companies_list=companies_list if isinstance(companies_list, list) else [],
                           week_summaries=week_summaries,
                           company_id=company_id,
                           sunday_anchor=sunday_anchor)


# ─────────────────────────────────────────────────────────────
# MENU BUILDER
# ─────────────────────────────────────────────────────────────
@app.route('/menubuilder')
@admin_required
def menubuilder():
    return render_template('menubuilder.html')


# ─────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    threading.Thread(target=_warmup_gas, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
