#!/usr/bin/env python3
"""
Seed test data for BROCK company:
  - 4 fake employees
  - Orders for the last 2 weeks (week of Mar 16 and Mar 23, 2026)
  - Generate invoices for both weeks
"""
import requests, json, time

GAS_URL = "https://script.google.com/macros/s/AKfycbxKVyW7sguwUq3TYsk-xtIF2fLicefaxTwl_PHjQVjt5-OiBarPQ_nXb_0H927NXAMG0w/exec"
COMPANY_ID   = "BROCK"
COMPANY_NAME = "Brock Health Meal Program"

# Last two full week anchors (Sunday dates)
WEEKS = [
    {"sunday": "2026-03-15", "delivery": "2026-03-19"},  # week of Mar 16
    {"sunday": "2026-03-22", "delivery": "2026-03-26"},  # week of Mar 23
]

EMPLOYEES = [
    {"first_name": "Sarah",   "last_name": "Mitchell", "email": "sarah.mitchell@brocku.ca"},
    {"first_name": "James",   "last_name": "Okafor",   "email": "james.okafor@brocku.ca"},
    {"first_name": "Priya",   "last_name": "Sharma",   "email": "priya.sharma@brocku.ca"},
    {"first_name": "Dylan",   "last_name": "Tremblay", "email": "dylan.tremblay@brocku.ca"},
]

# Sample meals with tier/pricing info matching typical BetterDay tiers
# Tier1: emp pays $0, company pays $12 | Tier2: emp pays $4, company pays $10 | Tier3: emp pays $8, company pays $8
MEALS = [
    {"id": "meal-001", "name": "Teriyaki Shrimp Bowl",     "diet": "Meat",        "tier": "Tier1", "emp": "0.00",  "co": "12.00", "bd": "0.00"},
    {"id": "meal-002", "name": "Butter Chicken & Rice",    "diet": "Meat",        "tier": "Tier2", "emp": "4.00",  "co": "10.00", "bd": "0.00"},
    {"id": "meal-003", "name": "Roasted Veggie Buddha Bowl","diet": "Plant-Based", "tier": "Tier1", "emp": "0.00",  "co": "12.00", "bd": "0.00"},
    {"id": "meal-004", "name": "BBQ Pulled Pork Wrap",     "diet": "Meat",        "tier": "Tier3", "emp": "8.00",  "co": "8.00",  "bd": "0.00"},
    {"id": "meal-005", "name": "Greek Chicken Salad",      "diet": "Meat",        "tier": "Tier2", "emp": "4.00",  "co": "10.00", "bd": "0.00"},
]

def gas(payload):
    r = requests.post(GAS_URL, json=payload, timeout=30)
    try:
        return r.json()
    except Exception:
        print(f"  [raw response] {r.text[:200]}")
        return {}

def main():
    print("=== BetterDay Test Data Seed ===\n")

    # 1. Register employees
    print("── Step 1: Register employees ──")
    for emp in EMPLOYEES:
        res = gas({
            "action": "register_employee",
            "company_id": COMPANY_ID,
            "first_name": emp["first_name"],
            "last_name":  emp["last_name"],
            "email":      emp["email"],
        })
        status = "already exists" if res.get("exists") else ("ok" if res.get("success") else f"FAIL: {res}")
        print(f"  {emp['first_name']} {emp['last_name']} ({emp['email']}) → {status}")
        time.sleep(0.5)

    print()

    # 2. Submit orders — each employee orders 1–2 meals each week
    print("── Step 2: Submit orders ──")
    # Each employee gets ONE order_id per week; multi-meal employees share it
    order_assignments = [
        # (employee_index, [meal_indices])
        (0, [0, 2]),   # Sarah: 2 meals
        (1, [1]),      # James: 1 meal
        (2, [3, 4]),   # Priya: 2 meals
        (3, [0]),      # Dylan: 1 meal
    ]

    for week in WEEKS:
        print(f"\n  Week of {week['sunday']} (delivery {week['delivery']}):")
        for emp_idx, meal_indices in order_assignments:
            emp = EMPLOYEES[emp_idx]
            # Reserve one order_id for this employee+week (same as real app flow)
            res_id = gas({
                "action":        "reserve_order_id",
                "email":         emp["email"],
                "sunday_anchor": week["sunday"],
            })
            order_id = str(res_id.get("order_id", ""))
            print(f"    Reserved order_id {order_id} for {emp['first_name']}")
            for meal_idx in meal_indices:
                meal = MEALS[meal_idx]
                res = gas({
                    "action":           "submit_corporate_order",
                    "company_id":       COMPANY_ID,
                    "company_name":     COMPANY_NAME,
                    "delivery_date":    week["delivery"],
                    "sunday_anchor":    week["sunday"],
                    "employee_name":    f"{emp['first_name']} {emp['last_name']}",
                    "employee_email":   emp["email"],
                    "meal_id":          meal["id"],
                    "dish_name":        meal["name"],
                    "diet_type":        meal["diet"],
                    "tier":             meal["tier"],
                    "employee_price":   meal["emp"],
                    "company_coverage": meal["co"],
                    "bd_coverage":      meal["bd"],
                    "order_id":         order_id,
                })
                status = "ok" if res.get("success") else f"FAIL: {res}"
                print(f"    {emp['first_name']:8} → {meal['name']:32} ({meal['tier']})  {status}")
                time.sleep(0.4)

    print()

    # 3. Generate invoices for both weeks
    print("── Step 3: Generate invoices ──")
    for week in WEEKS:
        res = gas({
            "action":        "generate_invoice",
            "company_id":    COMPANY_ID,
            "sunday_anchor": week["sunday"],
        })
        if res.get("success"):
            print(f"  Invoice created: {res.get('invoiceId', '?')}")
        elif res.get("skipped"):
            print(f"  Invoice for {week['sunday']} already exists — skipped")
        else:
            print(f"  Invoice {week['sunday']} → FAIL: {res}")
        time.sleep(1)

    print("\n=== Done! Reload the manager dashboard to see the data. ===")

if __name__ == "__main__":
    main()
