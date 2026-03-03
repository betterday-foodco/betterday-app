from flask import Flask, render_template, request, make_response, redirect, url_for, Response
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import os
import calendar
import re
import csv
import io

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxKVyW7sguwUq3TYsk-xtIF2fLicefaxTwl_PHjQVjt5-OiBarPQ_nXb_0H927NXAMG0w/exec"
TEACHER_SHEET_URL = "https://docs.google.com/spreadsheets"


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def get_nice_date(date_str):
    try:
        dt = datetime.strptime(str(date_str).split('T')[0], '%Y-%m-%d')
        return dt.strftime('%A, %b %d')
    except:
        return date_str

def format_week_header(date_str):
    try:
        dt = datetime.strptime(str(date_str), '%Y-%m-%d')
        return dt.strftime('%b %d, %Y')
    except:
        return date_str

def get_sunday_anchor(delivery_date_str):
    try:
        clean_date = str(delivery_date_str).split('T')[0]
        delivery_date = datetime.strptime(clean_date, '%Y-%m-%d')
        days_to_subtract = (delivery_date.weekday() + 1) % 7
        if days_to_subtract == 0:
            days_to_subtract = 7
        return (delivery_date - timedelta(days=days_to_subtract)).strftime('%Y-%m-%d')
    except:
        return None

def get_deadline_obj(delivery_date_str):
    try:
        clean_date = str(delivery_date_str).split('T')[0]
        delivery_date = datetime.strptime(clean_date, '%Y-%m-%d')
        days_to_subtract = (delivery_date.weekday() - 2) % 7
        if days_to_subtract <= 2:
            days_to_subtract += 7
        deadline_date = delivery_date - timedelta(days=days_to_subtract)
        return deadline_date.replace(hour=16, minute=0, second=0)
    except:
        return None

@app.template_filter('decode_school')
def decode_school_filter(s):
    return str(s).replace('+', ' ')


# ─────────────────────────────────────────────────────────────
# SCHOOL BOOKING — PUBLIC CALENDAR
# ─────────────────────────────────────────────────────────────
@app.route('/')
def index():
    booked_date_raw = request.cookies.get('user_booked_date')
    booked_date_nice = get_nice_date(booked_date_raw) if booked_date_raw else None

    taken_dates = []
    try:
        r_taken = requests.get(GOOGLE_SCRIPT_URL + "?action=get_bookings", timeout=8)
        taken_raw = r_taken.json() if r_taken.status_code == 200 else []
        if isinstance(taken_raw, list):
            for row in taken_raw:
                if isinstance(row, list) and len(row) > 0 and "Date" not in str(row[0]):
                    taken_dates.append(str(row[0]).split('T')[0])
    except:
        pass

    try:
        r_block = requests.post(GOOGLE_SCRIPT_URL, json={"action": "get_blocked_dates"}, timeout=8)
        blocked_dates = r_block.json() if r_block.status_code == 200 else []
    except:
        blocked_dates = []

    all_unavailable_dates = set(taken_dates + blocked_dates)

    start_date = datetime(2026, 3, 9)
    today = datetime.now()
    today_date = today.replace(hour=0, minute=0, second=0, microsecond=0)

    if today_date > start_date:
        start_date = today_date - timedelta(days=today_date.weekday())

    weeks = []
    for i in range(10):
        monday = start_date + timedelta(weeks=i)
        tuesday = monday + timedelta(days=1)
        wednesday = monday + timedelta(days=2)

        days = []
        for d in [monday, tuesday, wednesday]:
            d_str = d.strftime('%Y-%m-%d')
            days.append({
                'raw_date': d_str,
                'display': d.strftime('%A, %b %d'),
                'blocked': d_str in all_unavailable_dates,
                'past': d < today_date
            })

        weeks.append({
            'week_label': monday.strftime('Week of %b %d'),
            'days': days
        })

    return render_template('index.html', weeks=weeks, booked_date=booked_date_nice)


@app.route('/book/<date_raw>', methods=['GET', 'POST'])
def book(date_raw):
    if request.method == 'GET':
        return render_template('form.html', date_display=date_raw, raw_date=date_raw)
    data = {
        "action": "book_principal", "date": date_raw,
        "contact_name": request.form.get("contact_name"), "email": request.form.get("email"),
        "school_name": request.form.get("school_name"), "address": request.form.get("address"),
        "staff_count": request.form.get("staff_count"), "lunch_time": request.form.get("lunch_time"),
        "delivery_notes": request.form.get("delivery_notes")
    }
    try:
        requests.post(GOOGLE_SCRIPT_URL, json=data, timeout=10)
    except:
        pass

    resp = make_response(redirect(url_for('index')))
    resp.set_cookie('user_booked_date', date_raw, max_age=60*60*24*30)
    return resp


# ─────────────────────────────────────────────────────────────
# ADMIN DASHBOARD
# ─────────────────────────────────────────────────────────────
@app.route('/BD-Admin')
def bd_admin():
    try:
        r_books = requests.get(GOOGLE_SCRIPT_URL + "?action=get_bookings", timeout=10)
        bookings_raw = r_books.json() if r_books.status_code == 200 else []
    except:
        bookings_raw = []

    try:
        r_orders = requests.post(GOOGLE_SCRIPT_URL, json={"action": "get_all_orders"}, timeout=10)
        all_orders = r_orders.json() if r_orders.status_code == 200 else []
    except:
        all_orders = []

    try:
        r_block = requests.post(GOOGLE_SCRIPT_URL, json={"action": "get_blocked_dates"}, timeout=8)
        blocked_dates = r_block.json() if r_block.status_code == 200 else []
    except:
        blocked_dates = []

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
                if not isinstance(b, list) or len(b) < 3: continue
                if "Date" in str(b[0]) or str(b[2]).isdigit(): continue

                d_date = str(b[0]).split('T')[0]
                anchor = get_sunday_anchor(d_date)
                if not anchor: continue

                deadline_obj = get_deadline_obj(d_date)
                school_name = str(b[2])
                is_office = "Health" in school_name or "Headversity" in school_name
                meals_ordered = order_counts.get(f"{school_name}_{d_date}", 0)

                booking_obj = {
                    "delivery_date_raw": d_date, "delivery_date_display": get_nice_date(d_date),
                    "school": school_name, "status": str(b[7]) if len(b) > 7 else "New Booking",
                    "staff_count": str(b[4]) if len(b) > 4 else "0", "meals_ordered": meals_ordered,
                    "deadline": deadline_obj.strftime('%b %d') if deadline_obj else "TBD",
                    "type": "Office" if is_office else "School"
                }

                if anchor not in production_weeks:
                    production_weeks[anchor] = {
                        "nice_date": format_week_header(anchor),
                        "anchor_id": anchor,
                        "bookings": []
                    }
                production_weeks[anchor]["bookings"].append(booking_obj)
            except:
                continue

    sorted_weeks = dict(sorted(production_weeks.items()))

    start_date = datetime(2026, 3, 9)
    today = datetime.now()
    today_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
    if today_date > start_date:
        start_date = today_date - timedelta(days=today_date.weekday())

    toggle_weeks = []
    for i in range(10):
        monday = start_date + timedelta(weeks=i)
        tuesday = monday + timedelta(days=1)
        wednesday = monday + timedelta(days=2)

        days = []
        for d in [monday, tuesday, wednesday]:
            d_str = d.strftime('%Y-%m-%d')
            days.append({
                'raw_date': d_str,
                'display': d.strftime('%a, %b %d'),
                'blocked': d_str in blocked_dates,
                'past': d < today_date
            })

        toggle_weeks.append({
            'week_label': monday.strftime('Week of %b %d'),
            'days': days
        })

    return render_template('admin.html', weeks=sorted_weeks, toggle_weeks=toggle_weeks)


@app.route('/toggle-date', methods=['POST'])
def toggle_date():
    date_raw = request.form.get('date')
    try:
        requests.post(GOOGLE_SCRIPT_URL, json={"action": "toggle_block_date", "date": date_raw}, timeout=8)
    except:
        pass
    return "OK", 200


# ─────────────────────────────────────────────────────────────
# CULINARY & INVOICES
# ─────────────────────────────────────────────────────────────
@app.route('/culinary-summary/<sunday>')
def culinary_summary(sunday):
    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json={"action": "get_all_orders"}, timeout=20)
        all_orders = r.json()
    except:
        all_orders = []

    totals = {"Meat": {}, "Plant-Based": {}}
    total_count = 0

    if isinstance(all_orders, list):
        for o in all_orders:
            if not isinstance(o, dict): continue

            anchor = get_sunday_anchor(o.get('date'))
            if anchor == sunday:
                dish_name = str(o.get('dish_name') or f"Dish #{o.get('meal_id')}")
                diet = str(o.get('diet') or "Unknown")

                cat = "Plant-Based" if "Plant" in diet or "Vegan" in diet else "Meat"
                if dish_name not in totals[cat]:
                    totals[cat][dish_name] = 0

                totals[cat][dish_name] += 1
                total_count += 1

    for cat in totals:
        totals[cat] = dict(sorted(totals[cat].items()))

    return render_template('culinary_picklist.html', sunday=format_week_header(sunday), totals=totals, total_count=total_count)


@app.route('/batch-invoices/<sunday>')
def batch_invoices(sunday):
    try:
        r_books = requests.get(GOOGLE_SCRIPT_URL + "?action=get_bookings", timeout=10)
        bookings_raw = r_books.json() if r_books.status_code == 200 else []
    except:
        bookings_raw = []

    try:
        r_orders = requests.post(GOOGLE_SCRIPT_URL, json={"action": "get_all_orders"}, timeout=10)
        all_orders = r_orders.json() if r_orders.status_code == 200 else []
    except:
        all_orders = []

    schools_this_week = {}
    if isinstance(bookings_raw, list):
        for b in bookings_raw:
            try:
                if not isinstance(b, list) or len(b) < 3: continue
                if "Date" in str(b[0]) or str(b[2]).isdigit(): continue
                d_date = str(b[0]).split('T')[0]
                anchor = get_sunday_anchor(d_date)
                if anchor == sunday:
                    school_name = str(b[2])
                    schools_this_week[school_name] = get_nice_date(d_date)
            except:
                continue

    summaries = {}
    for school_name, nice_date in schools_this_week.items():
        summaries[school_name] = {"delivery_date": nice_date, "dishes": {}, "total": 0}

    if isinstance(all_orders, list):
        for o in all_orders:
            if not isinstance(o, dict): continue
            school_name = o.get('school')
            anchor = get_sunday_anchor(o.get('date'))

            if anchor == sunday and school_name in summaries:
                dish_name = str(o.get('dish_name') or f"Dish #{o.get('meal_id')}")
                if dish_name not in summaries[school_name]["dishes"]:
                    summaries[school_name]["dishes"][dish_name] = 0

                summaries[school_name]["dishes"][dish_name] += 1
                summaries[school_name]["total"] += 1

    return render_template('batch_invoices.html', sunday=format_week_header(sunday), summaries=summaries)


# ─────────────────────────────────────────────────────────────
# SCHOOL PROFILE & ORDER MANAGEMENT
# ─────────────────────────────────────────────────────────────
@app.route('/school-profile/<school_name>/<date>')
def school_profile(school_name, date):
    clean_school_name = school_name.replace('+', ' ')
    data = {}
    try:
        payload = {"action": "get_profile_data", "school": clean_school_name, "date": date}
        r = requests.post(GOOGLE_SCRIPT_URL, json=payload, timeout=12)
        data = r.json()
    except:
        pass

    deadline_obj = get_deadline_obj(date)
    days_left = (deadline_obj - datetime.now()).days if deadline_obj else -1
    deadline_str = deadline_obj.strftime('%b %d @ 4:00 PM') if deadline_obj else "TBD"

    if days_left < 0:   countdown_text = "⚠️ Orders Closed"
    elif days_left == 0: countdown_text = "🚨 Ends Today!"
    else:                countdown_text = f"⏰ {days_left} Days Left"

    return render_template('profile.html',
                           school=clean_school_name, date=date, display_date=get_nice_date(date),
                           deadline=deadline_str, countdown=countdown_text,
                           staff=int(data.get('staff_count', 0) if isinstance(data, dict) else 0),
                           orders=len(data.get('orders', []) if isinstance(data, dict) else []),
                           info=data if isinstance(data, dict) else {})


@app.route('/update-booking', methods=['POST'])
def update_booking():
    school = request.form.get('school')
    date = request.form.get('date')
    try:
        requests.post(GOOGLE_SCRIPT_URL, json={
            "action": "update_booking", "school": school, "date": date,
            "status": request.form.get('status'), "email": request.form.get('email')
        }, timeout=8)
    except:
        pass
    return redirect(url_for('school_profile', school_name=school.replace(' ', '+'), date=date))


@app.route('/download-csv/<school_name>/<date>')
def download_csv(school_name, date):
    clean_school_name = school_name.replace('+', ' ')
    try:
        payload = {"action": "get_profile_data", "school": clean_school_name, "date": date}
        r = requests.post(GOOGLE_SCRIPT_URL, json=payload)
        orders = r.json().get('orders', []) if isinstance(r.json(), dict) else []
    except:
        orders = []

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['Teacher Name', 'Diet', 'Dish ID', 'Dish Name'])
    for o in orders:
        if not isinstance(o, dict): continue
        mid = str(o.get('meal_id', '')).strip()
        d_name = o.get('dish_name', 'Unknown Dish')
        d_diet = o.get('diet', 'Unknown')
        cw.writerow([o.get('teacher', ''), d_diet, mid, d_name])

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = f"attachment; filename={clean_school_name}_orders.csv"
    output.headers["Content-type"] = "text/csv"
    return output


@app.route('/picklist/<school_name>/<date>')
def picklist_print(school_name, date):
    clean_school_name = school_name.replace('+', ' ')
    try:
        payload = {"action": "get_profile_data", "school": clean_school_name, "date": date}
        r = requests.post(GOOGLE_SCRIPT_URL, json=payload)
        orders = r.json().get('orders', []) if isinstance(r.json(), dict) else []
    except:
        orders = []

    summary = {}
    for o in orders:
        if not isinstance(o, dict): continue
        mid = str(o.get('meal_id', '')).strip()
        name = o.get('dish_name') or f"Dish #{mid}"
        if name not in summary:
            summary[name] = {"id": mid, "count": 0}
        summary[name]["count"] += 1

    return render_template('picklist.html', school=clean_school_name, date=date, summary=summary, total=len(orders))


# ─────────────────────────────────────────────────────────────
# TEACHER ORDERING
# ─────────────────────────────────────────────────────────────
@app.route('/order/<delivery_date>')
def teacher_order(delivery_date):
    if request.cookies.get(f'ordered_{delivery_date}'):
        school = request.args.get('school', 'BetterDay School')
        share_link = url_for('teacher_order', delivery_date=delivery_date, school=school, _external=True)
        return render_template('order_success.html', share_link=share_link, existing=True)

    anchor = get_sunday_anchor(delivery_date)
    school = request.args.get('school', 'BetterDay School')
    deadline_obj = get_deadline_obj(delivery_date)
    deadline_str = deadline_obj.strftime('%b %d @ 4:00 PM') if deadline_obj else "TBD"

    meat_menu = []
    vegan_menu = []
    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json={"action": "get_menu", "sunday_anchor": anchor}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                meat_menu = data.get('meat', [])
                vegan_menu = data.get('vegan', [])
    except:
        pass

    return render_template('orderform.html',
                           delivery_date=delivery_date, deadline=deadline_str,
                           meat_menu=meat_menu, vegan_menu=vegan_menu, school_name=school)


@app.route('/submit-order', methods=['POST'])
def submit_order():
    school = request.form.get('school_name')
    date = request.form.get('delivery_date')
    try:
        requests.post(GOOGLE_SCRIPT_URL, json={
            "action": "submit_teacher_order",
            "name": request.form.get('teacher_name'),
            "meal_id": request.form.get('meal_id'),
            "dish_name": request.form.get('dish_name'),
            "diet": request.form.get('dish_diet'),
            "delivery_date": date,
            "school": school,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }, timeout=5)
    except:
        pass

    share_link = url_for('teacher_order', delivery_date=date, school=school, _external=True)
    resp = make_response(render_template('order_success.html', share_link=share_link, existing=False))
    resp.set_cookie(f'ordered_{date}', 'true', max_age=60*60*24*30)
    return resp


# ─────────────────────────────────────────────────────────────
# BETTERDAY FOR WORK — CORPORATE EMPLOYEE ORDERING
# ─────────────────────────────────────────────────────────────
@app.route('/work')
def work_order():
    """Employee-facing corporate ordering portal."""
    return render_template('work.html', script_url=GOOGLE_SCRIPT_URL)


@app.route('/work/submit', methods=['POST'])
def work_submit():
    """
    Server-side proxy for corporate order submission.
    The work.html also submits directly via JS fetch() as primary path.
    This route is the fallback and future Stripe integration point.
    """
    data = request.get_json(force=True) or {}
    try:
        requests.post(GOOGLE_SCRIPT_URL, json={
            "action":           "submit_corporate_order",
            "company_id":       data.get("company_id"),
            "company_name":     data.get("company_name"),
            "delivery_date":    data.get("delivery_date"),
            "sunday_anchor":    data.get("sunday_anchor"),
            "employee_name":    data.get("employee_name"),
            "meal_id":          data.get("meal_id"),
            "dish_name":        data.get("dish_name"),
            "diet_type":        data.get("diet_type"),
            "tier":             data.get("tier"),
            "employee_price":   data.get("employee_price"),
            "company_coverage": data.get("company_coverage"),
            "bd_coverage":      data.get("bd_coverage", "0.00"),
        }, timeout=10)
        return {"status": "ok"}, 200
    except Exception as ex:
        return {"status": "error", "message": str(ex)}, 500


@app.route('/work/admin')
def work_admin():
    """
    Corporate orders summary dashboard.
    Filter by: ?company_id=BROCK  or  ?sunday=2026-03-08
    """
    company_id    = request.args.get('company_id', '')
    sunday_anchor = request.args.get('sunday', '')

    payload = {"action": "get_corporate_orders"}
    if company_id:    payload["company_id"]    = company_id
    if sunday_anchor: payload["sunday_anchor"] = sunday_anchor

    orders = []
    try:
        r = requests.post(GOOGLE_SCRIPT_URL, json=payload, timeout=10)
        orders = r.json() if r.status_code == 200 else []
    except:
        pass

    # Group by company → delivery date for the template
    grouped = defaultdict(lambda: defaultdict(list))
    for o in orders:
        if isinstance(o, dict):
            grouped[o.get('CompanyName', '—')][o.get('DeliveryDate', '—')].append(o)

    # Convert to plain dicts so Jinja can iterate safely
    grouped_plain = {
        company: dict(weeks)
        for company, weeks in grouped.items()
    }

    return render_template('work_admin.html',
                           grouped=grouped_plain,
                           company_id=company_id,
                           sunday_anchor=sunday_anchor)


# ─────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5001)))
