/**
 * BETTERDAY BACKEND SCRIPT v12
 * Added: Employee auth system
 *   - get_employee_by_email
 *   - register_employee
 *   - verify_company_pin
 *   - verify_magic_token / create_magic_token
 *
 * New sheets required in Hub spreadsheet:
 *   - Employees      : EmployeeID, CompanyID, FirstName, LastName, Email, CreatedAt, StripeCustomerID
 *   - CompanyPINs    : CompanyID, CompanyPin, UpdatedAt
 *   - MagicTokens    : Token, Email, CompanyID, CreatedAt, UsedAt (for real email flow later)
 *
 * New column required in Companies sheet:
 *   - CompanyEmailDomain  e.g. "brockhealth.com"  (leave blank if company has no domain)
 */
const BUFFER_SHEET_ID = "1iI6q2j7fYIcO5Da959RQeOr5BMFunP-VjsIwvNHA8Cg";
function doGet(e) {
  var ssHub = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ssHub.getSheetByName("Sheet1");
  var data = sheet.getDataRange().getValues();
  if (e && e.parameter && e.parameter.action === "get_bookings") {
    return ContentService.createTextOutput(JSON.stringify(data)).setMimeType(ContentService.MimeType.JSON);
  }
  var takenDates = [];
  for (var i = 1; i < data.length; i++) {
    if (data[i][0]) {
      var date = new Date(data[i][0]);
      takenDates.push(Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd"));
    }
  }
  return ContentService.createTextOutput(JSON.stringify(takenDates)).setMimeType(ContentService.MimeType.JSON);
}
function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var ssHub = SpreadsheetApp.getActiveSpreadsheet();
    var sheet1 = ssHub.getSheetByName("Sheet1");
    // ─────────────────────────────────────────
    // GET COMPANY
    // ─────────────────────────────────────────
    if (data.action === "get_company") {
      var compSheet = ssHub.getSheetByName("Companies");
      if (!compSheet) return jsonOut({error: "Companies sheet not found"});
      var rows = compSheet.getDataRange().getValues();
      var headers = rows[0];
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim().toUpperCase() === String(data.company_id).trim().toUpperCase()) {
          var company = {};
          headers.forEach(function(h, idx) { company[h] = rows[i][idx]; });
          return jsonOut({found: true, company: company});
        }
      }
      return jsonOut({found: false});
    }
    // ─────────────────────────────────────────
    // GET ALL COMPANIES (lightweight list for client-side prefetch)
    // ─────────────────────────────────────────
    if (data.action === "get_all_companies") {
      var compSheet = ssHub.getSheetByName("Companies");
      if (!compSheet) return jsonOut({companies: []});
      var rows = compSheet.getDataRange().getValues();
      var headers = rows[0];
      var idIdx   = headers.indexOf("CompanyID");
      var nameIdx = headers.indexOf("CompanyName");
      if (idIdx < 0 || nameIdx < 0) return jsonOut({companies: []});
      var list = [];
      for (var i = 1; i < rows.length; i++) {
        var id = String(rows[i][idIdx] || "").trim();
        if (!id) continue;
        var company = {};
        headers.forEach(function(h, idx) { company[h] = rows[i][idx]; });
        list.push(company);
      }
      return jsonOut({companies: list});
    }
    // ─────────────────────────────────────────
    // GET EMPLOYEE BY EMAIL
    // ─────────────────────────────────────────
    if (data.action === "get_employee_by_email") {
      var empSheet = getOrCreateEmployeesSheet(ssHub);
      var rows = empSheet.getDataRange().getValues();
      var headers = rows[0];
      var email = String(data.email).trim().toLowerCase();
      var companyId = String(data.company_id).trim().toUpperCase();
      var isManagerIdx = headers.indexOf("IsManager");
      for (var i = 1; i < rows.length; i++) {
        var rowEmail   = String(rows[i][4]).trim().toLowerCase();
        var rowCompany = String(rows[i][1]).trim().toUpperCase();
        if (rowEmail === email && rowCompany === companyId) {
          return jsonOut({
            found: true,
            employee: {
              firstName: rows[i][2],
              lastName:  rows[i][3],
              email:     rows[i][4],
              isManager: isManagerIdx >= 0 && rows[i][isManagerIdx] === true
            }
          });
        }
      }
      return jsonOut({found: false});
    }
    // ─────────────────────────────────────────
    // REGISTER EMPLOYEE
    // ─────────────────────────────────────────
    if (data.action === "register_employee") {
      var empSheet = getOrCreateEmployeesSheet(ssHub);
      var rows = empSheet.getDataRange().getValues();
      var email = String(data.email).trim().toLowerCase();
      var companyId = String(data.company_id).trim().toUpperCase();
      // Check for duplicate — email is col index 4
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][4]).trim().toLowerCase() === email && String(rows[i][1]).trim().toUpperCase() === companyId) {
          return jsonOut({success: false, exists: true});
        }
      }
      // Generate employee ID
      var empId = "EMP" + new Date().getTime().toString().slice(-8);
      // Append new employee
      // Columns: EmployeeID, CompanyID, FirstName, LastName, Email, CreatedAt, StripeCustomerID
      empSheet.appendRow([
        empId,
        companyId,
        String(data.first_name).trim(),
        String(data.last_name).trim(),
        email,
        new Date(),
        "" // StripeCustomerID — filled later when they add a card
      ]);
      return jsonOut({success: true, employeeId: empId});
    }
    // ─────────────────────────────────────────
    // VERIFY COMPANY PIN
    // ─────────────────────────────────────────
    if (data.action === "verify_company_pin") {
      var pinSheet = getOrCreatePINSheet(ssHub);
      var rows = pinSheet.getDataRange().getValues();
      var companyId = String(data.company_id).trim().toUpperCase();
      var incoming = String(data.pin).trim();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim().toUpperCase() === companyId) {
          return jsonOut({valid: String(rows[i][1]).trim() === incoming});
        }
      }
      return jsonOut({valid: false, error: "No PIN configured for this company"});
    }
    // ─────────────────────────────────────────
    // UPDATE COMPANY PIN  (called from manager dashboard)
    // ─────────────────────────────────────────
    if (data.action === "update_company_pin") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var newPin    = String(data.pin || "").trim();
      if (!newPin) return jsonOut({success: false, error: "PIN cannot be empty"});
      var pinSheet = getOrCreatePINSheet(ssHub);
      var rows = pinSheet.getDataRange().getValues();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim().toUpperCase() === companyId) {
          pinSheet.getRange(i + 1, 2).setValue(newPin);
          pinSheet.getRange(i + 1, 3).setValue(new Date());
          return jsonOut({success: true});
        }
      }
      pinSheet.appendRow([companyId, newPin, new Date()]);
      return jsonOut({success: true});
    }
    // ─────────────────────────────────────────
    // CREATE MAGIC TOKEN  (called when sign-in email is requested)
    // ─────────────────────────────────────────
    if (data.action === "create_magic_token") {
      var tokenSheet = getOrCreateTokenSheet(ssHub);
      var email = String(data.email).trim().toLowerCase();
      var companyId = String(data.company_id).trim().toUpperCase();
      // Token and URL are pre-built by Flask so the URL is always correct.
      // Fall back to GAS generation only if not provided (shouldn't happen in production).
      var token = data.token_override || (Utilities.getUuid().replace(/-/g, '') + Utilities.getUuid().replace(/-/g, ''));
      tokenSheet.appendRow([token, email, companyId, new Date(), ""]);
      // Send branded sign-in email via MailApp
      try {
        var APP_URL = PropertiesService.getScriptProperties().getProperty("APP_URL") || "https://betterday-app.onrender.com";
        var signInUrl = data.sign_in_url || (APP_URL + "/work?token=" + token + "&co=" + companyId);
        MailApp.sendEmail({
          to: email,
          subject: "Your BetterDay sign-in link",
          body: "Click the link below to sign in:\n\n" + signInUrl + "\n\nExpires in 15 minutes. Didn't request this? Ignore it.",
          htmlBody:
            "<!DOCTYPE html><html><body style='margin:0;padding:0;background:#f4ede3;font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif;'>" +
            "<table width='100%' cellpadding='0' cellspacing='0' style='background:#f4ede3;padding:40px 16px;'><tr><td align='center'>" +
            "<table width='480' cellpadding='0' cellspacing='0' style='max-width:480px;width:100%;'>" +
            // Header
            "<tr><td style='background:#4ea2fd;border-radius:16px 16px 0 0;padding:28px 32px;text-align:center;'>" +
            "<div style='font-family:Georgia,\"Times New Roman\",serif;font-size:1.5rem;color:#ffffff;font-weight:700;letter-spacing:-0.5px;'>BetterDay</div>" +
            "<div style='font-size:.65rem;color:rgba(255,255,255,.75);letter-spacing:2px;text-transform:uppercase;margin-top:3px;'>FOR WORK</div>" +
            "</td></tr>" +
            // Body
            "<tr><td style='background:#ffffff;padding:36px 32px 28px;'>" +
            "<p style='font-size:1.15rem;font-weight:800;color:#0d2030;margin:0 0 10px;'>Your sign-in link is ready</p>" +
            "<p style='font-size:.9rem;color:#50657a;line-height:1.65;margin:0 0 28px;'>Click the button below to sign in — no password needed. This link expires in <strong>15 minutes</strong> and can only be used once.</p>" +
            "<a href='" + signInUrl + "' style='display:block;background:#4ea2fd;color:#ffffff;text-decoration:none;padding:16px 24px;border-radius:12px;text-align:center;font-weight:700;font-size:1rem;letter-spacing:0.2px;'>Sign in to BetterDay &rarr;</a>" +
            "</td></tr>" +
            // Footer
            "<tr><td style='background:#f9f5f0;border-radius:0 0 16px 16px;padding:20px 32px;border-top:1px solid #e8e0d5;'>" +
            "<p style='font-size:.75rem;color:#9aabb8;margin:0;line-height:1.6;'>If you didn&rsquo;t request this, you can safely ignore it &mdash; your account is secure.<br>Questions? Reply to this email.</p>" +
            "</td></tr>" +
            "</table></td></tr></table></body></html>"
        });
      } catch(mailErr) {
        Logger.log("Magic link email failed: " + mailErr.toString());
      }
      return jsonOut({success: true}); // Token never returned to client
    }
    // ─────────────────────────────────────────
    // VERIFY MAGIC TOKEN  (called when user lands from email link)
    // ─────────────────────────────────────────
    if (data.action === "verify_magic_token") {
      var tokenSheet = getOrCreateTokenSheet(ssHub);
      var rows = tokenSheet.getDataRange().getValues();
      var token = String(data.token).trim();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim() === token) {
          // Check not already used
          if (rows[i][4]) return jsonOut({valid: false, error: "Token already used"});
          // Check not expired (15 min window)
          var created = new Date(rows[i][3]);
          var now = new Date();
          if ((now - created) > 15 * 60 * 1000) return jsonOut({valid: false, error: "Token expired"});
          // Mark as used
          tokenSheet.getRange(i + 1, 5).setValue(new Date());
          var email = rows[i][1];
          var companyId = rows[i][2];
          // Look up employee — email is col index 4
          var empSheet = getOrCreateEmployeesSheet(ssHub);
          var empRows = empSheet.getDataRange().getValues();
          var empHeaders = empRows[0];
          var isManagerIdx = empHeaders.indexOf("IsManager");
          for (var j = 1; j < empRows.length; j++) {
            if (String(empRows[j][4]).trim().toLowerCase() === email.toLowerCase() &&
                String(empRows[j][1]).trim().toUpperCase() === companyId.toUpperCase()) {
              // Look up company
              var compSheet = ssHub.getSheetByName("Companies");
              var compRows = compSheet.getDataRange().getValues();
              var compHeaders = compRows[0];
              var company = null;
              for (var k = 1; k < compRows.length; k++) {
                if (String(compRows[k][0]).trim().toUpperCase() === companyId.toUpperCase()) {
                  company = {};
                  compHeaders.forEach(function(h, idx) { company[h] = compRows[k][idx]; });
                  break;
                }
              }
              return jsonOut({
                valid: true,
                employee: {
                  firstName: empRows[j][2], lastName: empRows[j][3], email: email,
                  isManager: isManagerIdx >= 0 && empRows[j][isManagerIdx] === true
                },
                company: company
              });
            }
          }
          return jsonOut({valid: false, error: "Employee not found"});
        }
      }
      return jsonOut({valid: false, error: "Token not found"});
    }
    // ─────────────────────────────────────────
    // CREATE MANAGER TOKEN  (magic link for office managers)
    // ─────────────────────────────────────────
    if (data.action === "create_manager_token") {
      var email = String(data.email || "").trim().toLowerCase();
      var compSheet = ssHub.getSheetByName("Companies");
      if (!compSheet) return jsonOut({success: false, error: "Companies sheet not found"});
      var compRows = compSheet.getDataRange().getValues();
      var compHeaders = compRows[0];
      var primaryEmailIdx = compHeaders.indexOf("PrimaryContactEmail");
      var billingEmailIdx = compHeaders.indexOf("BillingContactEmail");
      var companyIdIdx    = compHeaders.indexOf("CompanyID");
      var companyNameIdx  = compHeaders.indexOf("CompanyName");
      var foundCompany = null;
      for (var i = 1; i < compRows.length; i++) {
        var primary = primaryEmailIdx >= 0 ? String(compRows[i][primaryEmailIdx] || "").trim().toLowerCase() : "";
        var billing = billingEmailIdx >= 0 ? String(compRows[i][billingEmailIdx] || "").trim().toLowerCase() : "";
        if (primary === email || billing === email) {
          foundCompany = { id: String(compRows[i][companyIdIdx]), name: String(compRows[i][companyNameIdx] || "") };
          break;
        }
      }
      if (!foundCompany) return jsonOut({success: false, error: "not_found"});
      var token = Utilities.getUuid().replace(/-/g, '') + Utilities.getUuid().replace(/-/g, '');
      var tokenSheet = getOrCreateManagerTokenSheet(ssHub);
      tokenSheet.appendRow([token, email, foundCompany.id, new Date(), ""]);
      try {
        var APP_URL = PropertiesService.getScriptProperties().getProperty("APP_URL") || "https://betterday-app.onrender.com";
        var signInUrl = APP_URL + "/manager?token=" + token;
        MailApp.sendEmail({
          to: email,
          subject: "Your BetterDay Manager sign-in link",
          htmlBody:
            "<!DOCTYPE html><html><body style='margin:0;padding:0;background:#f4ede3;font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif;'>" +
            "<table width='100%' cellpadding='0' cellspacing='0' style='background:#f4ede3;padding:40px 16px;'><tr><td align='center'>" +
            "<table width='480' cellpadding='0' cellspacing='0' style='max-width:480px;width:100%;'>" +
            "<tr><td style='background:#00465e;border-radius:16px 16px 0 0;padding:28px 32px;text-align:center;'>" +
            "<div style='font-family:Georgia,serif;font-size:1.5rem;color:#faebda;font-weight:700;'>BetterDay</div>" +
            "<div style='font-size:.65rem;color:rgba(250,235,218,.6);letter-spacing:2px;text-transform:uppercase;margin-top:3px;'>MANAGER PORTAL</div>" +
            "</td></tr>" +
            "<tr><td style='background:#fff;padding:36px 32px 28px;'>" +
            "<p style='font-size:1.1rem;font-weight:800;color:#0d2030;margin:0 0 10px;'>Your manager sign-in link</p>" +
            "<p style='font-size:.9rem;color:#50657a;line-height:1.65;margin:0 0 28px;'>Click below to access the <strong>" + foundCompany.name + "</strong> manager portal. This link expires in <strong>15 minutes</strong>.</p>" +
            "<a href='" + signInUrl + "' style='display:block;background:#00465e;color:#fff;text-decoration:none;padding:16px 24px;border-radius:12px;text-align:center;font-weight:700;font-size:1rem;'>Sign in to Manager Portal &rarr;</a>" +
            "</td></tr>" +
            "<tr><td style='background:#f9f5f0;border-radius:0 0 16px 16px;padding:20px 32px;border-top:1px solid #e8e0d5;'>" +
            "<p style='font-size:.75rem;color:#9aabb8;margin:0;'>Didn&rsquo;t request this? You can safely ignore it.</p>" +
            "</td></tr></table></td></tr></table></body></html>"
        });
      } catch(mailErr) { Logger.log("Manager magic link email failed: " + mailErr.toString()); }
      return jsonOut({success: true});
    }
    // ─────────────────────────────────────────
    // VERIFY MANAGER TOKEN
    // ─────────────────────────────────────────
    if (data.action === "verify_manager_token") {
      var tokenSheet = getOrCreateManagerTokenSheet(ssHub);
      var rows = tokenSheet.getDataRange().getValues();
      var token = String(data.token || "").trim();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim() !== token) continue;
        if (rows[i][4]) return jsonOut({valid: false, error: "Token already used"});
        var created = new Date(rows[i][3]);
        if ((new Date() - created) > 15 * 60 * 1000) return jsonOut({valid: false, error: "Token expired"});
        tokenSheet.getRange(i + 1, 5).setValue(new Date());
        var email = String(rows[i][1]);
        var companyId = String(rows[i][2]).trim().toUpperCase();
        var compSheet = ssHub.getSheetByName("Companies");
        var compRows = compSheet.getDataRange().getValues();
        var compHeaders = compRows[0];
        for (var k = 1; k < compRows.length; k++) {
          if (String(compRows[k][0]).trim().toUpperCase() === companyId) {
            var company = {};
            compHeaders.forEach(function(h, idx) { company[h] = compRows[k][idx]; });
            return jsonOut({valid: true, email: email, company: company});
          }
        }
        return jsonOut({valid: false, error: "Company not found"});
      }
      return jsonOut({valid: false, error: "Token not found"});
    }
    // ─────────────────────────────────────────
    // CREATE MANAGER SESSION  (gate screen → dashboard, no email needed)
    // Employee is already authenticated; just verify IsManager and issue token
    // ─────────────────────────────────────────
    if (data.action === "create_manager_session") {
      var email = String(data.email || "").trim().toLowerCase();
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var empSheet = getOrCreateEmployeesSheet(ssHub);
      var empRows = empSheet.getDataRange().getValues();
      var empHeaders = empRows[0];
      var isManagerIdx = empHeaders.indexOf("IsManager");
      var authorized = false;
      for (var i = 1; i < empRows.length; i++) {
        if (String(empRows[i][4]).trim().toLowerCase() === email &&
            String(empRows[i][1]).trim().toUpperCase() === companyId) {
          authorized = isManagerIdx >= 0 && empRows[i][isManagerIdx] === true;
          break;
        }
      }
      if (!authorized) return jsonOut({success: false, error: "Not authorized"});
      var token = Utilities.getUuid().replace(/-/g, '') + Utilities.getUuid().replace(/-/g, '');
      var tokenSheet = getOrCreateManagerTokenSheet(ssHub);
      tokenSheet.appendRow([token, email, companyId, new Date(), ""]);
      return jsonOut({success: true, token: token});
    }
    // ─────────────────────────────────────────
    // GET WEEK ORDER COUNTS (how many meals employee already placed per week)
    // ─────────────────────────────────────────
    if (data.action === "get_week_order_counts") {
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (!corpSheet) return jsonOut({counts: {}});
      var rows = corpSheet.getDataRange().getValues();
      var headers = rows[0];
      var emailIdx  = headers.indexOf("EmployeeEmail");
      var anchorIdx = headers.indexOf("SundayAnchor");
      if (emailIdx < 0 || anchorIdx < 0) return jsonOut({counts: {}});
      var email = String(data.email || "").trim().toLowerCase();
      var tz = Session.getScriptTimeZone();
      var counts = {};
      for (var i = 1; i < rows.length; i++) {
        if (!rows[i][0]) continue;
        var rowEmail = String(rows[i][emailIdx]).trim().toLowerCase();
        if (rowEmail !== email) continue;
        var raw = rows[i][anchorIdx];
        var anchor = (Object.prototype.toString.call(raw) === "[object Date]")
          ? Utilities.formatDate(raw, tz, "yyyy-MM-dd")
          : String(raw).trim();
        counts[anchor] = (counts[anchor] || 0) + 1;
      }
      return jsonOut({counts: counts});
    }
    // ─────────────────────────────────────────
    // GET ORDERS BY EMPLOYEE (for profile screen)
    // ─────────────────────────────────────────
    if (data.action === "get_orders_by_employee") {
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (!corpSheet) return jsonOut([]);
      var rows = corpSheet.getDataRange().getValues();
      var headers = rows[0];
      var orders = [];
      var emailFilter = String(data.email || "").trim().toLowerCase();
      for (var i = 1; i < rows.length; i++) {
        if (!rows[i][0]) continue;
        var rowEmail = String(rows[i][6]).trim().toLowerCase(); // EmployeeEmail col
        if (emailFilter && rowEmail !== emailFilter) continue;
        var order = {};
        headers.forEach(function(h, idx) {
          var val = rows[i][idx];
          if (Object.prototype.toString.call(val) === "[object Date]")
            val = Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
          order[h] = val;
        });
        orders.push(order);
      }
      // Return most recent first, max 20
      orders.reverse();
      if (orders.length > 20) orders = orders.slice(0, 20);
      return jsonOut(orders);
    }
    // ─────────────────────────────────────────
    // RESERVE ORDER ID  (call once per week before submitting meals)
    // Returns an existing OrderID for this employee+week, or creates a new one
    // ─────────────────────────────────────────
    if (data.action === "reserve_order_id") {
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      var email  = String(data.email  || "").trim().toLowerCase();
      var anchor = String(data.sunday_anchor || "").trim();
      if (corpSheet) {
        var rows = corpSheet.getDataRange().getValues();
        var headers = rows[0];
        var orderIdIdx = headers.indexOf("OrderID");
        var emailIdx   = headers.indexOf("EmployeeEmail");
        var anchorIdx  = headers.indexOf("SundayAnchor");
        if (orderIdIdx >= 0 && emailIdx >= 0 && anchorIdx >= 0) {
          for (var i = 1; i < rows.length; i++) {
            if (rows[i][orderIdIdx] &&
                String(rows[i][emailIdx]).trim().toLowerCase() === email &&
                String(rows[i][anchorIdx]).trim() === anchor) {
              return jsonOut({ order_id: rows[i][orderIdIdx] });
            }
          }
        }
      }
      return jsonOut({ order_id: getNextOrderId(ssHub) });
    }
    // ─────────────────────────────────────────
    // SUBMIT CORPORATE ORDER
    // ─────────────────────────────────────────
    if (data.action === "submit_corporate_order") {
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (!corpSheet) {
        corpSheet = ssHub.insertSheet("CorporateOrders");
        corpSheet.appendRow(["Timestamp","CompanyID","CompanyName","DeliveryDate","SundayAnchor","EmployeeName","EmployeeEmail","MealID","DishName","DietType","Tier","EmployeePrice","CompanyCoverage","BDCoverage","StripePaymentIntentID","Status","OrderID"]);
      }
      corpSheet.appendRow([
        new Date(),
        data.company_id,
        data.company_name,
        data.delivery_date,
        data.sunday_anchor,
        data.employee_name,
        data.employee_email || "",
        data.meal_id,
        data.dish_name,
        data.diet_type,
        data.tier,
        data.employee_price,
        data.company_coverage,
        data.bd_coverage || "0.00",
        "",
        "pending",
        data.order_id || ""
      ]);
      return jsonOut({success: true});
    }
    // ─────────────────────────────────────────
    // SWAP ORDER MEAL  (SKU swap — replace one meal in an existing order)
    // ─────────────────────────────────────────
    if (data.action === "swap_order_meal") {
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (!corpSheet) return jsonOut({success: false, error: "No orders sheet"});
      var rows = corpSheet.getDataRange().getValues();
      var headers = rows[0];
      var orderIdIdx  = headers.indexOf("OrderID");
      var emailIdx    = headers.indexOf("EmployeeEmail");
      var mealIdIdx   = headers.indexOf("MealID");
      var dishNameIdx = headers.indexOf("DishName");
      var dietIdx     = headers.indexOf("DietType");
      if (orderIdIdx < 0) return jsonOut({success: false, error: "OrderID column not found"});
      if (mealIdIdx < 0)  return jsonOut({success: false, error: "MealID column not found in sheet headers"});
      var orderId   = String(data.order_id).trim();
      var oldMealId = String(data.old_meal_id).trim();
      var email     = String(data.email || "").trim().toLowerCase();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][orderIdIdx]).trim() === orderId &&
            String(rows[i][emailIdx]).trim().toLowerCase() === email &&
            String(rows[i][mealIdIdx]).trim() === oldMealId) {
          corpSheet.getRange(i + 1, mealIdIdx + 1).setValue(String(data.new_meal_id).trim());
          if (dishNameIdx >= 0) corpSheet.getRange(i + 1, dishNameIdx + 1).setValue(data.new_dish_name || "");
          if (dietIdx     >= 0) corpSheet.getRange(i + 1, dietIdx     + 1).setValue(data.new_diet_type || "");
          return jsonOut({success: true});
        }
      }
      return jsonOut({success: false, error: "Meal not found in order"});
    }
    // ─────────────────────────────────────────
    // UPDATE EMPLOYEE EMAIL
    // ─────────────────────────────────────────
    if (data.action === "update_employee_email") {
      var empSheet = getOrCreateEmployeesSheet(ssHub);
      var rows = empSheet.getDataRange().getValues();
      var oldEmail  = String(data.old_email  || "").trim().toLowerCase();
      var newEmail  = String(data.new_email  || "").trim().toLowerCase();
      var companyId = String(data.company_id || "").trim().toUpperCase();
      if (!newEmail || !newEmail.includes("@")) return jsonOut({success: false, error: "Invalid email address."});
      // Check new email not already in use for this company
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][4]).trim().toLowerCase() === newEmail &&
            String(rows[i][1]).trim().toUpperCase() === companyId) {
          return jsonOut({success: false, error: "That email is already in use."});
        }
      }
      // Find and update the employee row (Email is col index 4, 1-based col 5)
      var empRowIdx = -1;
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][4]).trim().toLowerCase() === oldEmail &&
            String(rows[i][1]).trim().toUpperCase() === companyId) {
          empSheet.getRange(i + 1, 5).setValue(newEmail);
          empRowIdx = i;
          break;
        }
      }
      if (empRowIdx < 0) return jsonOut({success: false, error: "Account not found."});
      // Update EmployeeEmail in CorporateOrders so order history stays linked
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (corpSheet) {
        var oRows = corpSheet.getDataRange().getValues();
        var oHeaders = oRows[0];
        var emailColIdx = oHeaders.indexOf("EmployeeEmail");
        if (emailColIdx >= 0) {
          for (var i = 1; i < oRows.length; i++) {
            if (String(oRows[i][emailColIdx]).trim().toLowerCase() === oldEmail) {
              corpSheet.getRange(i + 1, emailColIdx + 1).setValue(newEmail);
            }
          }
        }
      }
      return jsonOut({success: true});
    }
    // ─────────────────────────────────────────
    // GET CORPORATE ORDERS
    // ─────────────────────────────────────────
    if (data.action === "get_corporate_orders") {
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (!corpSheet) return jsonOut([]);
      var rows = corpSheet.getDataRange().getValues();
      var headers = rows[0];
      var orders = [];
      for (var i = 1; i < rows.length; i++) {
        if (!rows[i][0]) continue;
        var order = {};
        headers.forEach(function(h, idx) {
          var val = rows[i][idx];
          if (Object.prototype.toString.call(val) === "[object Date]")
            val = Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
          order[h] = val;
        });
        orders.push(order);
      }
      if (data.company_id) orders = orders.filter(function(o) { return o.CompanyID === data.company_id; });
      if (data.sunday_anchor) orders = orders.filter(function(o) { return o.SundayAnchor === data.sunday_anchor; });
      return jsonOut(orders);
    }
    // ─────────────────────────────────────────
    // GET MENU
    // ─────────────────────────────────────────
    if (data.action === "get_menu") {
      var ssBuffer = SpreadsheetApp.openById(BUFFER_SHEET_ID);
      var scheduleSheet = ssBuffer.getSheetByName("8.0 Menu Schedule");
      var schedRows = scheduleSheet.getDataRange().getValues();
      var sundayMatch = data.sunday_anchor;
      var meatIds = [], veganIds = [];
      // AI (index 34) = single cell with comma-separated meat IDs (e.g. "#509, #319, #508...")
      // AJ (index 35) = single cell with comma-separated vegan IDs (e.g. "#196, #517, #473...")
      var AI_COL = 34;
      var AJ_COL = 35;
      function extractIdsFromCell(cellVal) {
        if (!cellVal) return [];
        var ids = [];
        var matches = cellVal.toString().match(/#\d+/g);
        if (matches) matches.forEach(function(m) { ids.push(m); });
        return ids;
      }
      for (var i = 1; i < schedRows.length; i++) {
        // Column H (index 7) stores the SUNDAY delivery date directly.
        var cellVal = schedRows[i][7];
        if (!cellVal) continue;
        var sundayDate = new Date(cellVal);
        if (isNaN(sundayDate.getTime())) continue;
        // Fuzzy match: compare within ±24h to absorb timezone offsets between
        // the sheet's stored date and the UTC anchor sent from the frontend.
        // Using noon UTC as the reference makes the window symmetric.
        var sundayMatchMs = new Date(sundayMatch + 'T12:00:00Z').getTime();
        var diffMs = Math.abs(sundayDate.getTime() - sundayMatchMs);
        if (diffMs <= 24 * 60 * 60 * 1000) {
          meatIds  = extractIdsFromCell(schedRows[i][AI_COL]);
          veganIds = extractIdsFromCell(schedRows[i][AJ_COL]);
          break;
        }
      }
      var masterSheet = ssBuffer.getSheetByName("7.1 Dish Masterlist");
      var masterRows  = masterSheet.getDataRange().getValues();
      var dishMap = {};
      for (var m = 1; m < masterRows.length; m++) {
        var dId = String(masterRows[m][0]).trim();
        if (dId) {
          dishMap[dId] = {
            name:        masterRows[m][2],
            diet:        masterRows[m][3],
            image:       masterRows[m][21],
            description: masterRows[m][23],
            cal:         masterRows[m][24],
            protein:     masterRows[m][25],
            carbs:       masterRows[m][26],
            fat:         masterRows[m][27],
            tags:        masterRows[m][32]
          };
        }
      }
      var meatMenu = [], veganMenu = [];
      meatIds.forEach(function(id)  { if(dishMap[id]) meatMenu.push( {id:id, ...dishMap[id]}); });
      veganIds.forEach(function(id) { if(dishMap[id]) veganMenu.push({id:id, ...dishMap[id]}); });
      return jsonOut({meat: meatMenu, vegan: veganMenu});
    }
    // ─────────────────────────────────────────
    // ALL LEGACY ACTIONS — UNCHANGED
    // ─────────────────────────────────────────
    if (data.action === "get_profile_data") {
      var s1Rows = sheet1.getDataRange().getValues();
      var bookingData = {};
      for (var i = 1; i < s1Rows.length; i++) {
        var rDate = Utilities.formatDate(new Date(s1Rows[i][0]), Session.getScriptTimeZone(), "yyyy-MM-dd");
        if (s1Rows[i][2] == data.school && rDate == data.date) {
          bookingData = { contact: s1Rows[i][1], address: s1Rows[i][3], staff_count: s1Rows[i][4], lunch_hours: s1Rows[i][5], notes: s1Rows[i][6], status: s1Rows[i][7], email: s1Rows[i][8] || "" };
          break;
        }
      }
      var orderSheet = ssHub.getSheetByName("TeacherOrders");
      var orders = [];
      if (orderSheet) {
        var oRows = orderSheet.getDataRange().getValues();
        for (var j = 1; j < oRows.length; j++) {
          var oDate = oRows[j][2];
          if (oDate instanceof Date) oDate = Utilities.formatDate(oDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
          if (oRows[j][1] == data.school && oDate == data.date)
            orders.push({ teacher: oRows[j][3], meal_id: oRows[j][4], dish_name: oRows[j][5] || "", diet: oRows[j][6] || "" });
        }
      }
      bookingData.orders = orders;
      return jsonOut(bookingData);
    }
    if (data.action === "update_booking") {
      var rows = sheet1.getDataRange().getValues();
      for (var i = 1; i < rows.length; i++) {
        var rDate = Utilities.formatDate(new Date(rows[i][0]), Session.getScriptTimeZone(), "yyyy-MM-dd");
        if (rows[i][2] == data.school && rDate == data.date) {
          sheet1.getRange(i + 1, 8).setValue(data.status);
          sheet1.getRange(i + 1, 9).setValue(data.email);
          return ContentService.createTextOutput("Update Success");
        }
      }
      return ContentService.createTextOutput("Error: Booking not found");
    }
    if (data.action === "submit_teacher_order") {
      var orderSheet = ssHub.getSheetByName("TeacherOrders");
      if (!orderSheet) {
        orderSheet = ssHub.insertSheet("TeacherOrders");
        orderSheet.appendRow(["Timestamp","School","Delivery Date","Teacher Name","Meal ID","Dish Name","Diet Type"]);
      }
      orderSheet.appendRow([new Date(), data.school, data.delivery_date, data.name, data.meal_id, data.dish_name || ("Dish #" + data.meal_id), data.diet || "Unknown"]);
      return ContentService.createTextOutput("Order Success");
    }
    if (data.action === "book_principal") {
      sheet1.appendRow([data.date, data.contact_name, data.school_name, data.address, data.staff_count, data.lunch_time, data.delivery_notes, "🆕 New Booking", data.email]);
      return ContentService.createTextOutput("Booking Success");
    }
    if (data.action === "get_all_orders") {
      var orderSheet = ssHub.getSheetByName("TeacherOrders");
      var orders = [];
      if (orderSheet) {
        var rows = orderSheet.getDataRange().getValues();
        for (var i = 1; i < rows.length; i++) {
          var d = rows[i][2];
          if (Object.prototype.toString.call(d) === "[object Date]") d = Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
          orders.push({ school: rows[i][1], date: d, meal_id: rows[i][4], dish_name: rows[i][5] || "", diet: rows[i][6] || "" });
        }
      }
      return jsonOut(orders);
    }
    if (data.action === "get_blocked_dates") {
      var blockSheet = ssHub.getSheetByName("BlockedDates");
      if (!blockSheet) return jsonOut([]);
      var bRows = blockSheet.getDataRange().getValues();
      var blocked = [];
      for (var i = 1; i < bRows.length; i++) {
        var d = bRows[i][0];
        if (d) blocked.push(Object.prototype.toString.call(d) === "[object Date]" ? Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(d).trim());
      }
      return jsonOut(blocked);
    }
    if (data.action === "toggle_block_date") {
      var blockSheet = ssHub.getSheetByName("BlockedDates");
      if (!blockSheet) { blockSheet = ssHub.insertSheet("BlockedDates"); blockSheet.appendRow(["Blocked Date"]); }
      var bRows = blockSheet.getDataRange().getValues();
      var found = false;
      for (var i = 1; i < bRows.length; i++) {
        var d = bRows[i][0];
        var dStr = Object.prototype.toString.call(d) === "[object Date]" ? Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(d).trim();
        if (dStr === data.date) { blockSheet.deleteRow(i + 1); found = true; break; }
      }
      if (!found) blockSheet.appendRow([data.date]);
      return ContentService.createTextOutput("Toggled");
    }
    // ─────────────────────────────────────────
    // GET ALL COMPANIES
    // ─────────────────────────────────────────
    if (data.action === "get_all_companies") {
      var compSheet = ssHub.getSheetByName("Companies");
      if (!compSheet) return jsonOut([]);
      var rows = compSheet.getDataRange().getValues();
      var headers = rows[0];
      var companies = [];
      for (var i = 1; i < rows.length; i++) {
        if (!rows[i][0]) continue;
        var company = {};
        headers.forEach(function(h, idx) { company[h] = rows[i][idx]; });
        companies.push(company);
      }
      return jsonOut(companies);
    }
    // ─────────────────────────────────────────
    // SAVE COMPANY
    // ─────────────────────────────────────────
    if (data.action === "save_company") {
      var compSheet = ssHub.getSheetByName("Companies");
      if (!compSheet) {
        compSheet = ssHub.insertSheet("Companies");
      }
      var rows = compSheet.getDataRange().getValues();
      var headers = rows.length > 0 ? rows[0] : [];
      var fields = Object.assign({}, data);
      delete fields.action;
      var companyId = String(fields.CompanyID || "").trim().toUpperCase();
      if (!companyId) return jsonOut({success: false, error: "CompanyID required"});
      // Find existing row
      var rowIdx = -1;
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim().toUpperCase() === companyId) {
          rowIdx = i;
          break;
        }
      }
      if (rowIdx >= 0) {
        // Update existing — also add new columns if needed
        Object.keys(fields).forEach(function(key) {
          if (!key) return;
          var colIdx = headers.indexOf(key);
          if (colIdx >= 0) {
            compSheet.getRange(rowIdx + 1, colIdx + 1).setValue(fields[key]);
          } else {
            // New column — add header and value
            var newCol = headers.length + 1;
            compSheet.getRange(1, newCol).setValue(key);
            compSheet.getRange(rowIdx + 1, newCol).setValue(fields[key]);
            headers.push(key);
          }
        });
      } else {
        // New company
        if (headers.length === 0) {
          // Empty sheet — create headers from fields
          var newHeaders = Object.keys(fields);
          compSheet.appendRow(newHeaders);
          compSheet.appendRow(newHeaders.map(function(h) { return fields[h] !== undefined ? fields[h] : ""; }));
        } else {
          var newRow = headers.map(function(h) { return fields[h] !== undefined ? fields[h] : ""; });
          compSheet.appendRow(newRow);
        }
      }
      return jsonOut({success: true});
    }
    return ContentService.createTextOutput("Error: Unknown Action");
  } catch (err) {
    return ContentService.createTextOutput("Error: " + err.toString());
  }
}
// ══════════════════════════════════════════
// HELPER FUNCTIONS
// ══════════════════════════════════════════
function getOrCreateSettingsSheet(ssHub) {
  var sheet = ssHub.getSheetByName("Settings");
  if (!sheet) {
    sheet = ssHub.insertSheet("Settings");
    sheet.appendRow(["Key", "Value"]);
    sheet.appendRow(["LastOrderID", 10000]);
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
  }
  return sheet;
}
function getNextOrderId(ssHub) {
  var settings = getOrCreateSettingsSheet(ssHub);
  var rows = settings.getDataRange().getValues();
  for (var i = 1; i < rows.length; i++) {
    if (String(rows[i][0]) === "LastOrderID") {
      var next = parseInt(rows[i][1]) + 1;
      settings.getRange(i + 1, 2).setValue(next);
      return next;
    }
  }
  settings.appendRow(["LastOrderID", 10001]);
  return 10001;
}
function jsonOut(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}
function getAdminSecret() {
  // Store this in Script Properties: Extensions > Apps Script > Project Settings > Script Properties
  // Key: ADMIN_SECRET  Value: (choose something strong)
  try {
    return PropertiesService.getScriptProperties().getProperty("ADMIN_SECRET") || "changeme";
  } catch(e) {
    return "changeme";
  }
}
function getOrCreateEmployeesSheet(ssHub) {
  var sheet = ssHub.getSheetByName("Employees");
  if (!sheet) {
    sheet = ssHub.insertSheet("Employees");
    sheet.appendRow(["EmployeeID", "CompanyID", "FirstName", "LastName", "Email", "CreatedAt", "StripeCustomerID"]);
    // Freeze header row
    sheet.setFrozenRows(1);
    // Format header
    sheet.getRange(1, 1, 1, 7).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
  }
  return sheet;
}
function getOrCreatePINSheet(ssHub) {
  var sheet = ssHub.getSheetByName("CompanyPINs");
  if (!sheet) {
    sheet = ssHub.insertSheet("CompanyPINs");
    sheet.appendRow(["CompanyID", "CompanyPin", "UpdatedAt"]);
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, 3).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
    // Add a note explaining how to set PINs
    sheet.getRange("A1").setNote("Managers can update their PIN from the Manager Dashboard. Run setInitialPINs() once to seed all companies with a default PIN.");
  }
  return sheet;
}
function getOrCreateManagerTokenSheet(ssHub) {
  var sheet = ssHub.getSheetByName("ManagerTokens");
  if (!sheet) {
    sheet = ssHub.insertSheet("ManagerTokens");
    sheet.appendRow(["Token", "Email", "CompanyID", "CreatedAt", "UsedAt"]);
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, 5).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
  }
  return sheet;
}
/**
 * Run this once from the GAS editor (Run → setInitialPINs) to seed every company
 * in the Companies sheet with PIN "2026". Skips companies that already have a PIN.
 */
function setInitialPINs() {
  var ssHub   = SpreadsheetApp.getActiveSpreadsheet();
  var compSheet = ssHub.getSheetByName("Companies");
  if (!compSheet) { Logger.log("No Companies sheet found"); return; }
  var compRows    = compSheet.getDataRange().getValues();
  var compHeaders = compRows[0];
  var idIdx       = compHeaders.indexOf("CompanyID");
  if (idIdx < 0) { Logger.log("No CompanyID column found"); return; }

  var pinSheet  = getOrCreatePINSheet(ssHub);
  var pinRows   = pinSheet.getDataRange().getValues();
  // Build set of companies that already have a PIN
  var existing  = {};
  for (var i = 1; i < pinRows.length; i++) {
    existing[String(pinRows[i][0]).trim().toUpperCase()] = true;
  }

  var added = 0;
  for (var i = 1; i < compRows.length; i++) {
    var companyId = String(compRows[i][idIdx]).trim().toUpperCase();
    if (!companyId) continue;
    if (existing[companyId]) {
      Logger.log("Skipping " + companyId + " (already has PIN)");
      continue;
    }
    pinSheet.appendRow([companyId, "2026", new Date()]);
    Logger.log("Set PIN for " + companyId);
    added++;
  }
  Logger.log("Done — " + added + " PIN(s) added.");
}

function getOrCreateTokenSheet(ssHub) {
  var sheet = ssHub.getSheetByName("MagicTokens");
  if (!sheet) {
    sheet = ssHub.insertSheet("MagicTokens");
    sheet.appendRow(["Token", "Email", "CompanyID", "CreatedAt", "UsedAt"]);
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, 5).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
  }
  return sheet;
}
