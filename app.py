from flask import (Flask, render_template, request, make_response,
                   redirect, url_for, Response, session, jsonify)
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import os
import csv
import io
import logging
from functools import wraps

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
GOOGLE_SCRIPT_URL = os.environ.get(
    'GOOGLE_SCRIPT_URL',
    "https://script.google.com/macros/s/AKfycbxKVyW7sguwUq3TYsk-xtIF2fLicefaxTwl_PHjQVjt5-OiBarPQ_nXb_0H927NXAMG0w/exec"
)
ADMIN_PASSWORD  = os.environ.get('ADMIN_PASSWORD',   'betterday2024')
app.secret_key  = os.environ.get('FLASK_SECRET_KEY', 'bd-dev-secret-change-in-prod')

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


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
# BETTERDAY FOR WORK — CORPORATE EMPLOYEE ORDERING
# ─────────────────────────────────────────────────────────────
@app.route('/work')
def work_order():
    """Employee-facing corporate ordering portal.
    No script_url passed — all JS calls go through /api/gas proxy."""
    return render_template('work.html')


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
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
