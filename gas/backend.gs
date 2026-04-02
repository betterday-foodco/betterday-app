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
  return ContentService.createTextOutput(JSON.stringify({status: "ok"})).setMimeType(ContentService.MimeType.JSON);
}
function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var ssHub = SpreadsheetApp.getActiveSpreadsheet();
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
          // Attach benefit levels if enabled
          if (String(company.EnableEmployeeLevels || "").toUpperCase() === "TRUE") {
            company.benefitLevels = _getBenefitLevels(ssHub, String(data.company_id).trim().toUpperCase());
          }
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
      var benefitLevelIdx = headers.indexOf("BenefitLevel");
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
              isManager: isManagerIdx >= 0 && rows[i][isManagerIdx] === true,
              benefitLevel: benefitLevelIdx >= 0 ? (String(rows[i][benefitLevelIdx] || "General").trim()) : "General"
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
    // GET EMPLOYEES  (for manager dashboard employees tab)
    // ─────────────────────────────────────────
    if (data.action === "get_employees") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var empSheet  = getOrCreateEmployeesSheet(ssHub);
      var rows      = empSheet.getDataRange().getValues();
      var headers   = rows[0];
      var isManagerIdx = headers.indexOf("IsManager");
      var result = [];
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][1]).trim().toUpperCase() !== companyId) continue;
        result.push({
          employeeId: rows[i][0],
          firstName:  rows[i][2],
          lastName:   rows[i][3],
          email:      rows[i][4],
          createdAt:  rows[i][5] ? Utilities.formatDate(new Date(rows[i][5]), Session.getScriptTimeZone(), "yyyy-MM-dd") : "",
          isManager:  isManagerIdx >= 0 && rows[i][isManagerIdx] === true
        });
      }
      return jsonOut({employees: result});
    }
    // ─────────────────────────────────────────
    // REMOVE EMPLOYEE
    // ─────────────────────────────────────────
    if (data.action === "remove_employee") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var email     = String(data.email || "").trim().toLowerCase();
      var empSheet  = getOrCreateEmployeesSheet(ssHub);
      var rows      = empSheet.getDataRange().getValues();
      for (var i = rows.length - 1; i >= 1; i--) {
        if (String(rows[i][4]).trim().toLowerCase() === email &&
            String(rows[i][1]).trim().toUpperCase() === companyId) {
          empSheet.deleteRow(i + 1);
          return jsonOut({success: true});
        }
      }
      return jsonOut({success: false, error: "Employee not found"});
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
    // GET COMPANY PIN  (called by manager dashboard on load)
    // ─────────────────────────────────────────
    if (data.action === "get_company_pin") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var pinSheet  = getOrCreatePINSheet(ssHub);
      var rows      = pinSheet.getDataRange().getValues();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim().toUpperCase() === companyId) {
          return jsonOut({found: true, pin: String(rows[i][1]).trim()});
        }
      }
      return jsonOut({found: false, pin: ""});
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
            "<tr><td style='background:#00465e;border-radius:16px 16px 0 0;padding:28px 32px;text-align:center;'>" +
            "<img src='https://betterday-app.onrender.com/static/Cream%20Logo.png' alt='BetterDay' style='height:36px;display:block;margin:0 auto;'>" +
            "<div style='font-size:.6rem;color:rgba(250,235,218,.55);letter-spacing:2.5px;text-transform:uppercase;margin-top:5px;'>FOR WORK</div>" +
            "</td></tr>" +
            // Body
            "<tr><td style='background:#ffffff;padding:36px 32px 28px;'>" +
            "<p style='font-size:1.15rem;font-weight:800;color:#0d2030;margin:0 0 10px;'>Your sign-in link is ready</p>" +
            "<p style='font-size:.9rem;color:#50657a;line-height:1.65;margin:0 0 28px;'>Click the button below to sign in — no password needed. This link expires in <strong>15 minutes</strong> and can only be used once.</p>" +
            "<a href='" + signInUrl + "' style='display:block;background:#00465e;color:#ffffff;text-decoration:none;padding:16px 24px;border-radius:12px;text-align:center;font-weight:700;font-size:1rem;letter-spacing:0.2px;'>Sign in to BetterDay &rarr;</a>" +
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
            "<img src='https://betterday-app.onrender.com/static/Cream%20Logo.png' alt='BetterDay' style='height:32px;display:block;margin:0 auto;'>" +
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
            var rawRowAn = rows[i][anchorIdx];
            var rowAnchorStr = (rawRowAn instanceof Date)
              ? Utilities.formatDate(rawRowAn, Session.getScriptTimeZone(), "yyyy-MM-dd")
              : String(rawRowAn).trim();
            if (rows[i][orderIdIdx] &&
                String(rows[i][emailIdx]).trim().toLowerCase() === email &&
                rowAnchorStr === anchor) {
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
        corpSheet.appendRow(["Timestamp","CompanyID","CompanyName","DeliveryDate","SundayAnchor","EmployeeName","EmployeeEmail","MealID","DishName","DietType","Tier","EmployeePrice","CompanyCoverage","BDCoverage","PaymentTransactionID","Status","OrderID","EmployeeLevel"]);
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
        data.payment_transaction_id || "",
        data.status || "confirmed",
        data.order_id || "",
        data.employee_level || "General"
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
    // GET PAR LEVEL CATALOG (all items from 9.0 Merged Masterlist, grouped by category)
    // ─────────────────────────────────────────
    if (data.action === "get_par_catalog") {
      var ssBuffer = SpreadsheetApp.openById(BUFFER_SHEET_ID);
      var sheet = ssBuffer.getSheetByName("9.0 Merged Masterlist");
      if (!sheet) return jsonOut({error: "9.0 Merged Masterlist not found"});
      var rows = sheet.getDataRange().getValues();
      // Column indices: 0=ID, 2=Name, 3=Diet, 4=Active, 6=Type, 21=Photo URL, 23=Description, 24=Cal, 25=Pro, 26=Carb, 27=Fat, 32=Tags
      var typeMap = {
        "Omni - Meat":      "meat_entree",
        "Meat Only":        "meat_entree",
        "Omni - Vegan":     "plant_entree",
        "Vegan Only":       "plant_entree",
        "Breakfast":        "hot_breakfast",
        "Sandwich & Wraps": "sandwich_wrap",
        "Snack":            "snack",
        "Chia & Parfait":   "chia_oats",
        "Protein Pack":     "snack",
        "Bulk Prep":        "snack",
        "Cookie":           "cookie",
        "Juice":            "juice"
      };
      var catalog = {};
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][4]).trim() !== "Active") continue;
        var typeRaw = String(rows[i][6] || "").trim();
        var catId = typeMap[typeRaw];
        if (!catId) continue;
        if (!catalog[catId]) catalog[catId] = [];
        catalog[catId].push({
          id:    String(rows[i][0]).trim(),
          name:  String(rows[i][2] || "").trim(),
          diet:  String(rows[i][3] || "").trim(),
          type:  typeRaw,
          image: String(rows[i][21] || "").trim(),
          description: String(rows[i][23] || "").trim(),
          cal:   rows[i][24] || 0,
          protein: rows[i][25] || 0,
          carbs: rows[i][26] || 0,
          fat:   rows[i][27] || 0,
          tags:  String(rows[i][32] || "").trim()
        });
      }
      // Placeholder items for categories not yet extracted to 9.0 sheet
      var placeholders = {
        cookie: [
          {id:"#PH-C1", name:"Chocolate Chip Cookie", diet:"snack", type:"Cookie", image:"", description:"Classic chocolate chip, baked fresh.", cal:220, protein:3, carbs:28, fat:12, tags:""},
          {id:"#PH-C2", name:"Oatmeal Raisin Cookie", diet:"snack", type:"Cookie", image:"", description:"Chewy oatmeal with plump raisins.", cal:200, protein:3, carbs:30, fat:9, tags:""},
          {id:"#PH-C3", name:"Double Chocolate Cookie", diet:"snack", type:"Cookie", image:"", description:"Rich double chocolate indulgence.", cal:240, protein:4, carbs:32, fat:13, tags:""},
          {id:"#PH-C4", name:"Peanut Butter Cookie", diet:"snack", type:"Cookie", image:"", description:"Nutty, crumbly peanut butter classic.", cal:210, protein:5, carbs:24, fat:12, tags:"Gluten Free"}
        ],
        juice: [
          {id:"#PH-J1", name:"Green Machine Juice", diet:"juice", type:"Juice", image:"", description:"Spinach, apple, ginger, lemon.", cal:120, protein:2, carbs:28, fat:0, tags:"Dairy Free, Gluten Free"},
          {id:"#PH-J2", name:"Berry Blast Juice", diet:"juice", type:"Juice", image:"", description:"Mixed berries, banana, coconut water.", cal:140, protein:1, carbs:34, fat:0, tags:"Dairy Free, Gluten Free"},
          {id:"#PH-J3", name:"Orange Sunrise Juice", diet:"juice", type:"Juice", image:"", description:"Fresh orange, carrot, turmeric.", cal:110, protein:1, carbs:26, fat:0, tags:"Dairy Free, Gluten Free"},
          {id:"#PH-J4", name:"Tropical Mango Juice", diet:"juice", type:"Juice", image:"", description:"Mango, pineapple, lime, coconut.", cal:150, protein:1, carbs:36, fat:0, tags:"Dairy Free, Gluten Free"}
        ]
      };
      // Merge placeholders for empty categories
      Object.keys(placeholders).forEach(function(catId) {
        if (!catalog[catId] || catalog[catId].length === 0) {
          catalog[catId] = placeholders[catId];
        }
      });
      return jsonOut({catalog: catalog});
    }

    // ─────────────────────────────────────────
    // GET BENEFIT LEVELS (for a company)
    // ─────────────────────────────────────────
    if (data.action === "get_benefit_levels") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      return jsonOut({levels: _getBenefitLevels(ssHub, companyId)});
    }
    // ─────────────────────────────────────────
    // SAVE BENEFIT LEVELS (bulk replace for a company)
    // ─────────────────────────────────────────
    if (data.action === "save_benefit_levels") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      if (!companyId) return jsonOut({success: false, error: "CompanyID required"});
      var levels = data.levels || [];
      var sheet = ssHub.getSheetByName("BenefitLevels");
      if (!sheet) {
        sheet = ssHub.insertSheet("BenefitLevels");
        sheet.appendRow(["CompanyID","LevelID","LevelName","LevelOrder","FreeMealsPerWeek","FreeTier_EmployeePrice","FreeTier_BDSubsidy","FreeTier_CompanySubsidy","Tier1_Meals","Tier1_EmployeePrice","Tier1_BDSubsidy","Tier1_CompanySubsidy","Tier2_Meals","Tier2_EmployeePrice","Tier2_BDSubsidy","Tier2_CompanySubsidy","Tier3_Meals","Tier3_EmployeePrice","Tier3_BDSubsidy","Tier3_CompanySubsidy","MaxMealsPerWeek","FullPrice"]);
        sheet.setFrozenRows(1);
      }
      var rows = sheet.getDataRange().getValues();
      var headers = rows[0];
      // Delete existing rows for this company (bottom-up to avoid index shift)
      for (var i = rows.length - 1; i >= 1; i--) {
        if (String(rows[i][0]).trim().toUpperCase() === companyId) {
          sheet.deleteRow(i + 1);
        }
      }
      // Append new levels
      levels.forEach(function(lv, idx) {
        var row = headers.map(function(h) {
          if (h === "CompanyID") return companyId;
          if (h === "LevelOrder") return idx + 1;
          return lv[h] !== undefined ? lv[h] : "";
        });
        sheet.appendRow(row);
      });
      return jsonOut({success: true});
    }
    // ─────────────────────────────────────────
    // UPDATE EMPLOYEE BENEFIT LEVEL
    // ─────────────────────────────────────────
    if (data.action === "update_employee_level") {
      var empSheet = getOrCreateEmployeesSheet(ssHub);
      var rows = empSheet.getDataRange().getValues();
      var headers = rows[0];
      var emailIdx = headers.indexOf("Email");
      var coIdx = headers.indexOf("CompanyID");
      var levelIdx = headers.indexOf("BenefitLevel");
      if (levelIdx < 0) {
        levelIdx = headers.length;
        empSheet.getRange(1, levelIdx + 1).setValue("BenefitLevel");
      }
      var email = String(data.email || "").trim().toLowerCase();
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var newLevel = String(data.benefit_level || "General").trim();
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][emailIdx]).trim().toLowerCase() === email &&
            String(rows[i][coIdx]).trim().toUpperCase() === companyId) {
          empSheet.getRange(i + 1, levelIdx + 1).setValue(newLevel);
          return jsonOut({success: true});
        }
      }
      return jsonOut({success: false, error: "Employee not found"});
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
    // ─────────────────────────────────────────
    // GET INVOICES  (manager portal — past weeks only)
    // ─────────────────────────────────────────
    if (data.action === "get_invoices") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var invSheet  = getOrCreateInvoiceSheet(ssHub);
      var rows      = invSheet.getDataRange().getValues();
      var headers   = rows[0];
      var companyIdIdx = headers.indexOf("CompanyID");
      var result    = [];
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][companyIdIdx]).trim().toUpperCase() !== companyId) continue;
        result.push(_readInvoiceRow(headers, rows[i]));
      }
      result.sort(function(a,b){ return (b.PeriodStart || "").localeCompare(a.PeriodStart || ""); });
      return jsonOut({invoices: result});
    }
    // ─────────────────────────────────────────
    // GET ALL INVOICES  (BD admin — all companies)
    // ─────────────────────────────────────────
    if (data.action === "get_all_invoices") {
      var invSheet = getOrCreateInvoiceSheet(ssHub);
      var rows     = invSheet.getDataRange().getValues();
      var headers  = rows[0];
      var invoiceNumberIdx = headers.indexOf("InvoiceNumber");
      var result   = [];
      for (var i = 1; i < rows.length; i++) {
        if (!rows[i][invoiceNumberIdx] && !rows[i][headers.indexOf("InvoiceID")]) continue;
        result.push(_readInvoiceRow(headers, rows[i]));
      }
      result.sort(function(a,b){ return (b.PeriodStart || "").localeCompare(a.PeriodStart || ""); });
      return jsonOut({invoices: result});
    }
    // ─────────────────────────────────────────
    // UPDATE INVOICE STATUS  (admin — mark paid/sent)
    // ─────────────────────────────────────────
    if (data.action === "update_invoice_status") {
      var invoiceId      = String(data.invoice_id || "").trim();
      var newStatus      = String(data.status || "").trim();
      var paymentMethod  = String(data.payment_method || "").trim();
      var paidAmount     = data.paid_amount;
      var paymentRef     = String(data.payment_reference || "").trim();
      var notes          = String(data.notes || "").trim();
      var invSheet       = getOrCreateInvoiceSheet(ssHub);
      var rows           = invSheet.getDataRange().getValues();
      var headers        = rows[0];
      var invoiceIdIdx   = headers.indexOf("InvoiceID");
      var statusIdx      = headers.indexOf("Status");
      var paidAtIdx      = headers.indexOf("PaidAt");
      var sentAtIdx      = headers.indexOf("SentAt");
      var paidAmountIdx  = headers.indexOf("PaidAmount");
      var payRefIdx      = headers.indexOf("PaymentReference");
      var payMethodIdx   = headers.indexOf("PaymentMethod");
      var notesIdx       = headers.indexOf("Notes");
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][invoiceIdIdx]).trim() !== invoiceId) continue;
        if (statusIdx >= 0) invSheet.getRange(i+1, statusIdx+1).setValue(newStatus);
        if (newStatus === "paid" && paidAtIdx >= 0) invSheet.getRange(i+1, paidAtIdx+1).setValue(new Date());
        if (newStatus === "sent" && sentAtIdx >= 0) invSheet.getRange(i+1, sentAtIdx+1).setValue(new Date());
        if (paymentMethod && payMethodIdx >= 0) invSheet.getRange(i+1, payMethodIdx+1).setValue(paymentMethod);
        if (paidAmount !== undefined && paidAmount !== "" && paidAmountIdx >= 0) invSheet.getRange(i+1, paidAmountIdx+1).setValue(parseFloat(paidAmount) || 0);
        if (paymentRef && payRefIdx >= 0) invSheet.getRange(i+1, payRefIdx+1).setValue(paymentRef);
        if (notes && notesIdx >= 0) invSheet.getRange(i+1, notesIdx+1).setValue(notes);
        return jsonOut({success: true});
      }
      return jsonOut({success: false, error: "Invoice not found"});
    }
    // ─────────────────────────────────────────
    // GENERATE INVOICE  (admin trigger or auto-Thursday)
    // ─────────────────────────────────────────
    if (data.action === "generate_invoice") {
      var sundayAnchor = String(data.sunday_anchor || "").trim();
      var companyId    = String(data.company_id || "").trim().toUpperCase();
      if (!sundayAnchor || !companyId) return jsonOut({success:false, error:"Missing params"});
      var result = _buildInvoiceForCompany(ssHub, companyId, sundayAnchor, false);
      return jsonOut(result);
    }
    // ─────────────────────────────────────────
    // CREATE CREDIT NOTE
    // ─────────────────────────────────────────
    if (data.action === "create_credit_note") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var amount = parseFloat(data.amount) || 0;
      var reason = String(data.reason || "").trim();
      var createdBy = String(data.created_by || "admin").trim();
      if (!companyId || amount <= 0) return jsonOut({success:false, error:"CompanyID and positive amount required"});

      var cnSheet = ssHub.getSheetByName("CreditNotes");
      if (!cnSheet) {
        cnSheet = ssHub.insertSheet("CreditNotes");
        cnSheet.appendRow(["CreditNoteID","CompanyID","CompanyName","Amount","Reason","Status","AppliedToInvoice","CreatedAt","CreatedBy","Notes"]);
        cnSheet.setFrozenRows(1);
        cnSheet.getRange(1,1,1,10).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
      }
      // Look up company name
      var compSheet = ssHub.getSheetByName("Companies");
      var compRows = compSheet.getDataRange().getValues();
      var compHeaders = compRows[0];
      var coNameIdx = compHeaders.indexOf("CompanyName");
      var coIdIdx = compHeaders.indexOf("CompanyID");
      var creditBalIdx = compHeaders.indexOf("CreditBalance");
      var companyName = "", companyRow = -1;
      for (var k = 1; k < compRows.length; k++) {
        if (String(compRows[k][coIdIdx]).trim().toUpperCase() === companyId) {
          companyName = String(compRows[k][coNameIdx] || "");
          companyRow = k;
          break;
        }
      }
      // Generate credit note ID
      var cnRows = cnSheet.getDataRange().getValues();
      var cnCount = 0;
      for (var i = 1; i < cnRows.length; i++) {
        if (String(cnRows[i][1]).trim().toUpperCase() === companyId) cnCount++;
      }
      var cnId = "CN-" + companyId + "-" + String(cnCount + 1).padStart(3, "0");
      cnSheet.appendRow([cnId, companyId, companyName, amount, reason, "pending", "", new Date(), createdBy, data.notes || ""]);
      // Add to company's credit balance
      if (creditBalIdx >= 0 && companyRow >= 0) {
        var currentBal = parseFloat(compRows[companyRow][creditBalIdx]) || 0;
        compSheet.getRange(companyRow + 1, creditBalIdx + 1).setValue(Math.round((currentBal + amount) * 100) / 100);
      }
      return jsonOut({success:true, creditNoteId:cnId});
    }
    // ─────────────────────────────────────────
    // GET CREDIT NOTES (for a company or all)
    // ─────────────────────────────────────────
    if (data.action === "get_credit_notes") {
      var cnSheet = ssHub.getSheetByName("CreditNotes");
      if (!cnSheet) return jsonOut({creditNotes:[]});
      var rows = cnSheet.getDataRange().getValues();
      var headers = rows[0];
      var companyId = data.company_id ? String(data.company_id).trim().toUpperCase() : null;
      var result = [];
      for (var i = 1; i < rows.length; i++) {
        if (!rows[i][0]) continue;
        if (companyId && String(rows[i][headers.indexOf("CompanyID")]).trim().toUpperCase() !== companyId) continue;
        var cn = {};
        headers.forEach(function(h, idx) {
          var val = rows[i][idx];
          if ((h === "CreatedAt") && val) {
            try { val = Utilities.formatDate(new Date(val), Session.getScriptTimeZone(), "yyyy-MM-dd"); } catch(e) {}
          }
          cn[h] = val !== undefined && val !== null ? val : "";
        });
        result.push(cn);
      }
      return jsonOut({creditNotes:result});
    }
    // ─────────────────────────────────────────
    // VOID CREDIT NOTE
    // ─────────────────────────────────────────
    if (data.action === "void_credit_note") {
      var cnId = String(data.credit_note_id || "").trim();
      var cnSheet = ssHub.getSheetByName("CreditNotes");
      if (!cnSheet) return jsonOut({success:false, error:"No CreditNotes sheet"});
      var rows = cnSheet.getDataRange().getValues();
      var headers = rows[0];
      var idIdx = headers.indexOf("CreditNoteID");
      var statusIdx = headers.indexOf("Status");
      var amountIdx = headers.indexOf("Amount");
      var coIdx = headers.indexOf("CompanyID");
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][idIdx]).trim() !== cnId) continue;
        if (String(rows[i][statusIdx]).trim() === "applied") return jsonOut({success:false, error:"Cannot void an applied credit"});
        cnSheet.getRange(i+1, statusIdx+1).setValue("void");
        // Subtract from company credit balance
        var amt = parseFloat(rows[i][amountIdx]) || 0;
        var companyId = String(rows[i][coIdx]).trim().toUpperCase();
        var compSheet = ssHub.getSheetByName("Companies");
        var compRows = compSheet.getDataRange().getValues();
        var compHeaders = compRows[0];
        var creditBalIdx = compHeaders.indexOf("CreditBalance");
        var compIdIdx = compHeaders.indexOf("CompanyID");
        for (var k = 1; k < compRows.length; k++) {
          if (String(compRows[k][compIdIdx]).trim().toUpperCase() === companyId) {
            var bal = parseFloat(compRows[k][creditBalIdx]) || 0;
            compSheet.getRange(k+1, creditBalIdx+1).setValue(Math.max(0, Math.round((bal - amt)*100)/100));
            break;
          }
        }
        return jsonOut({success:true});
      }
      return jsonOut({success:false, error:"Credit note not found"});
    }
    // ─────────────────────────────────────────
    // GET AR SUMMARY (aging report for admin dashboard)
    // ─────────────────────────────────────────
    if (data.action === "get_ar_summary") {
      var invSheet = getOrCreateInvoiceSheet(ssHub);
      var rows = invSheet.getDataRange().getValues();
      var headers = rows[0];
      var now = new Date();
      var summary = {totalOutstanding:0, current:0, over15:0, over30:0, over60:0, over90:0, byCompany:{}};
      for (var i = 1; i < rows.length; i++) {
        var inv = _readInvoiceRow(headers, rows[i]);
        if (inv.Status === "paid" || inv.Status === "void" || !inv.AmountDue) continue;
        var due = inv.AmountDue;
        var paidAmt = parseFloat(inv.PaidAmount) || 0;
        var outstanding = due - paidAmt;
        if (outstanding <= 0) continue;
        summary.totalOutstanding += outstanding;
        // Aging
        var dueDate = inv.DueDate ? new Date(inv.DueDate + "T12:00:00") : now;
        var daysOverdue = Math.floor((now - dueDate) / (1000*60*60*24));
        if (daysOverdue <= 0) summary.current += outstanding;
        else if (daysOverdue <= 15) summary.current += outstanding;
        else if (daysOverdue <= 30) summary.over15 += outstanding;
        else if (daysOverdue <= 60) summary.over30 += outstanding;
        else if (daysOverdue <= 90) summary.over60 += outstanding;
        else summary.over90 += outstanding;
        // By company
        var co = inv.CompanyID || "UNKNOWN";
        if (!summary.byCompany[co]) summary.byCompany[co] = {name:inv.CompanyName||co, outstanding:0, invoiceCount:0, oldestDue:""};
        summary.byCompany[co].outstanding += outstanding;
        summary.byCompany[co].invoiceCount++;
        if (!summary.byCompany[co].oldestDue || inv.DueDate < summary.byCompany[co].oldestDue) {
          summary.byCompany[co].oldestDue = inv.DueDate;
        }
      }
      summary.totalOutstanding = Math.round(summary.totalOutstanding*100)/100;
      summary.current = Math.round(summary.current*100)/100;
      summary.over15 = Math.round(summary.over15*100)/100;
      summary.over30 = Math.round(summary.over30*100)/100;
      summary.over60 = Math.round(summary.over60*100)/100;
      summary.over90 = Math.round(summary.over90*100)/100;
      return jsonOut(summary);
    }
    // ─────────────────────────────────────────
    // SEND ORDER REMINDERS (admin triggers manually)
    // Returns summary of who was emailed
    // ─────────────────────────────────────────
    if (data.action === "send_order_reminders") {
      var tz = Session.getScriptTimeZone();
      // Find current week's Sunday anchor
      var today = new Date();
      var dow = today.getDay();
      var sun = new Date(today);
      sun.setDate(today.getDate() - dow);
      var sundayAnchor = Utilities.formatDate(sun, tz, "yyyy-MM-dd");
      var deliveryMon = new Date(sun);
      deliveryMon.setDate(sun.getDate() + 1);
      var deliveryLabel = Utilities.formatDate(deliveryMon, tz, "MMMM d, yyyy");

      // Get all employees
      var empSheet = getOrCreateEmployeesSheet(ssHub);
      var empRows = empSheet.getDataRange().getValues();
      var empHeaders = empRows[0];

      // Get all orders for this week
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      var orderedEmails = {};
      if (corpSheet) {
        var oRows = corpSheet.getDataRange().getValues();
        var oHeaders = oRows[0];
        var oEmailIdx = oHeaders.indexOf("EmployeeEmail");
        var oAnchorIdx = oHeaders.indexOf("SundayAnchor");
        for (var i = 1; i < oRows.length; i++) {
          var rawAn = oRows[i][oAnchorIdx];
          var rowAn = (rawAn instanceof Date) ? Utilities.formatDate(rawAn, tz, "yyyy-MM-dd") : String(rawAn).trim();
          if (rowAn === sundayAnchor) {
            orderedEmails[String(oRows[i][oEmailIdx]).trim().toLowerCase()] = true;
          }
        }
      }

      // Find employees who haven't ordered
      var APP_URL = PropertiesService.getScriptProperties().getProperty("APP_URL") || "https://betterday-app.onrender.com";
      var sent = 0, skipped = 0, totalEmployees = 0, totalOrdered = 0;
      var companies = {};
      // Optional company filter — if provided, only send to these companies
      var filterCos = null;
      if (data.company_ids && data.company_ids.length > 0) {
        filterCos = {};
        data.company_ids.forEach(function(c) { filterCos[String(c).trim().toUpperCase()] = true; });
      }

      for (var i = 1; i < empRows.length; i++) {
        var email = String(empRows[i][4] || "").trim().toLowerCase();
        var coId = String(empRows[i][1] || "").trim().toUpperCase();
        if (!email || !coId) continue;
        if (filterCos && !filterCos[coId]) continue;
        totalEmployees++;
        if (!companies[coId]) companies[coId] = {total: 0, ordered: 0};
        companies[coId].total++;

        if (orderedEmails[email]) {
          totalOrdered++;
          companies[coId].ordered++;
          skipped++;
          continue;
        }

        // Send reminder email
        try {
          var firstName = String(empRows[i][2] || "").trim() || "there";
          MailApp.sendEmail({
            to: email,
            subject: "Don't forget to order your meals this week!",
            htmlBody:
              "<!DOCTYPE html><html><body style='margin:0;padding:0;background:#f4ede3;font-family:-apple-system,BlinkMacSystemFont,sans-serif;'>" +
              "<table width='100%' cellpadding='0' cellspacing='0' style='background:#f4ede3;padding:40px 16px;'><tr><td align='center'>" +
              "<table width='480' cellpadding='0' cellspacing='0' style='max-width:480px;width:100%;'>" +
              "<tr><td style='background:#00465e;border-radius:16px 16px 0 0;padding:28px 32px;text-align:center;'>" +
              "<img src='https://betterday-app.onrender.com/static/Cream%20Logo.png' alt='BetterDay' style='height:32px;display:block;margin:0 auto;'>" +
              "<div style='font-size:.65rem;color:rgba(250,235,218,.6);letter-spacing:2px;text-transform:uppercase;margin-top:3px;'>FOR WORK</div>" +
              "</td></tr>" +
              "<tr><td style='background:#fff;padding:36px 32px 28px;'>" +
              "<p style='font-size:1.15rem;font-weight:800;color:#0d2030;margin:0 0 10px;'>Hey " + firstName + ", your meals are waiting!</p>" +
              "<p style='font-size:.9rem;color:#50657a;line-height:1.65;margin:0 0 28px;'>This week's menu is live and orders close <strong>Wednesday at midnight</strong>. Delivery is <strong>" + deliveryLabel + "</strong>.</p>" +
              "<a href='" + APP_URL + "/work' style='display:block;background:#00465e;color:#fff;text-decoration:none;padding:16px 24px;border-radius:12px;text-align:center;font-weight:700;font-size:1rem;'>Order your meals &rarr;</a>" +
              "</td></tr>" +
              "<tr><td style='background:#f9f5f0;border-radius:0 0 16px 16px;padding:20px 32px;border-top:1px solid #e8e0d5;'>" +
              "<p style='font-size:.75rem;color:#9aabb8;margin:0;'>You're receiving this because you're enrolled in your company's BetterDay meal program.</p>" +
              "</td></tr></table></td></tr></table></body></html>"
          });
          sent++;
        } catch(mailErr) {
          Logger.log("Reminder email failed for " + email + ": " + mailErr.toString());
        }
      }

      return jsonOut({
        success: true,
        sent: sent,
        skipped: skipped,
        totalEmployees: totalEmployees,
        totalOrdered: totalOrdered,
        weekOf: sundayAnchor,
        deliveryDate: deliveryLabel
      });
    }

    // ─────────────────────────────────────────
    // MANAGER SAVE MEAL ALLOWANCES (with audit log)
    // ─────────────────────────────────────────
    if (data.action === "manager_save_meal_allowances") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var changedBy = String(data.changed_by || "manager").trim();
      var changes   = data.changes || [];
      if (!companyId || changes.length === 0) return jsonOut({success: false, error: "Missing data"});

      var compSheet = ssHub.getSheetByName("Companies");
      var compRows  = compSheet.getDataRange().getValues();
      var compHeaders = compRows[0];
      var compRowIdx  = -1;
      for (var i = 1; i < compRows.length; i++) {
        if (String(compRows[i][0]).trim().toUpperCase() === companyId) { compRowIdx = i; break; }
      }
      if (compRowIdx < 0) return jsonOut({success: false, error: "Company not found"});

      // Get or create MealEditLog sheet
      var logSheet = ssHub.getSheetByName("MealEditLog");
      if (!logSheet) {
        logSheet = ssHub.insertSheet("MealEditLog");
        logSheet.appendRow(["Timestamp","CompanyID","ChangedBy","LevelType","LevelName","Field","OldValue","NewValue","Description"]);
        logSheet.setFrozenRows(1);
        logSheet.getRange(1,1,1,9).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
      }

      var mealFields = ["FreeMealsPerWeek","Tier1_Meals","Tier2_Meals","Tier3_Meals","MaxMealsPerWeek"];
      var logEntries = [];

      changes.forEach(function(ch) {
        if (ch.level === "default") {
          // Update company-level defaults
          mealFields.forEach(function(f) {
            if (ch[f] === undefined) return;
            var colIdx = compHeaders.indexOf(f);
            if (colIdx < 0) return;
            var oldVal = String(compRows[compRowIdx][colIdx] || "0");
            var newVal = String(ch[f] || "0");
            if (oldVal !== newVal) {
              compSheet.getRange(compRowIdx + 1, colIdx + 1).setValue(ch[f]);
              logEntries.push([new Date(), companyId, changedBy, "default", "Default", f, oldVal, newVal,
                f + ": " + oldVal + " → " + newVal]);
            }
          });
        } else {
          // Update a benefit level row
          var lvSheet = ssHub.getSheetByName("BenefitLevels");
          if (!lvSheet) return;
          var lvRows = lvSheet.getDataRange().getValues();
          var lvHeaders = lvRows[0];
          for (var j = 1; j < lvRows.length; j++) {
            if (String(lvRows[j][0]).trim().toUpperCase() !== companyId) continue;
            if (String(lvRows[j][lvHeaders.indexOf("LevelID")]) !== String(ch.level)) continue;
            mealFields.forEach(function(f) {
              if (ch[f] === undefined) return;
              var colIdx = lvHeaders.indexOf(f);
              if (colIdx < 0) return;
              var oldVal = String(lvRows[j][colIdx] || "0");
              var newVal = String(ch[f] || "0");
              if (oldVal !== newVal) {
                lvSheet.getRange(j + 1, colIdx + 1).setValue(ch[f]);
                logEntries.push([new Date(), companyId, changedBy, "level", ch.levelName || "Level", f, oldVal, newVal,
                  (ch.levelName || "Level") + " " + f + ": " + oldVal + " → " + newVal]);
              }
            });
            break;
          }
        }
      });

      // Write all log entries
      logEntries.forEach(function(row) { logSheet.appendRow(row); });

      return jsonOut({success: true, changesLogged: logEntries.length});
    }

    // ─────────────────────────────────────────
    // GET MEAL CHANGE LOG
    // ─────────────────────────────────────────
    if (data.action === "get_meal_change_log") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var logSheet = ssHub.getSheetByName("MealEditLog");
      if (!logSheet) return jsonOut({log: []});
      var rows = logSheet.getDataRange().getValues();
      var result = [];
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][1]).trim().toUpperCase() !== companyId) continue;
        result.push({
          timestamp: rows[i][0] ? Utilities.formatDate(new Date(rows[i][0]), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm") : "",
          changedBy: String(rows[i][2] || ""),
          levelName: String(rows[i][4] || ""),
          field: String(rows[i][5] || ""),
          oldValue: String(rows[i][6] || ""),
          newValue: String(rows[i][7] || ""),
          description: String(rows[i][8] || "")
        });
      }
      // Most recent first, max 50
      result.reverse();
      if (result.length > 50) result = result.slice(0, 50);
      return jsonOut({log: result});
    }

    // ─────────────────────────────────────────
    // GET PAR LEVELS
    // ─────────────────────────────────────────
    if (data.action === "get_par_levels") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var sheet = ssHub.getSheetByName("ParLevels");
      if (!sheet) return jsonOut({levels: {}});
      var rows = sheet.getDataRange().getValues();
      var headers = rows[0];
      var levels = {};
      for (var i = 1; i < rows.length; i++) {
        if (String(rows[i][0]).trim().toUpperCase() !== companyId) continue;
        var catId = String(rows[i][1]).trim();
        levels[catId] = {
          qty: parseInt(rows[i][2]) || 0,
          status: String(rows[i][3] || "active").trim(),
          mode: String(rows[i][4] || "auto").trim(),
          items: rows[i][5] ? JSON.parse(rows[i][5]) : []
        };
      }
      return jsonOut({levels: levels});
    }

    // ─────────────────────────────────────────
    // SAVE PAR LEVELS
    // ─────────────────────────────────────────
    if (data.action === "save_par_levels") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      if (!companyId) return jsonOut({success: false, error: "CompanyID required"});
      var levels = data.levels || {};
      var sheet = ssHub.getSheetByName("ParLevels");
      if (!sheet) {
        sheet = ssHub.insertSheet("ParLevels");
        sheet.appendRow(["CompanyID","CategoryID","WeeklyQty","Status","Mode","ItemsJSON","LastModified","ModifiedBy"]);
        sheet.setFrozenRows(1);
        sheet.getRange(1,1,1,8).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
      }
      var rows = sheet.getDataRange().getValues();
      var headers = rows[0];
      // Delete existing rows for this company (bottom-up)
      for (var i = rows.length - 1; i >= 1; i--) {
        if (String(rows[i][0]).trim().toUpperCase() === companyId) {
          sheet.deleteRow(i + 1);
        }
      }
      // Append new rows
      var changedBy = String(data.changed_by || "manager").trim();
      Object.keys(levels).forEach(function(catId) {
        var lv = levels[catId];
        sheet.appendRow([
          companyId,
          catId,
          parseInt(lv.qty) || 0,
          String(lv.status || "active"),
          String(lv.mode || "auto"),
          lv.items ? JSON.stringify(lv.items) : "[]",
          new Date(),
          changedBy
        ]);
      });
      return jsonOut({success: true});
    }

    // ─────────────────────────────────────────
    // CONFIRM PAR ORDER (creates order rows for this week)
    // ─────────────────────────────────────────
    if (data.action === "confirm_par_order") {
      var companyId = String(data.company_id || "").trim().toUpperCase();
      var changedBy = String(data.changed_by || "manager").trim();
      var levels = data.levels || {};
      // Get company name
      var compSheet = ssHub.getSheetByName("Companies");
      var compRows = compSheet.getDataRange().getValues();
      var compHeaders = compRows[0];
      var compNameIdx = compHeaders.indexOf("CompanyName");
      var companyName = "";
      for (var i = 1; i < compRows.length; i++) {
        if (String(compRows[i][0]).trim().toUpperCase() === companyId) {
          companyName = String(compRows[i][compNameIdx] || "");
          break;
        }
      }
      // Calculate Sunday anchor (next Sunday)
      var today = new Date();
      var daysUntilSun = (7 - today.getDay()) % 7;
      if (daysUntilSun === 0) daysUntilSun = 7;
      var nextSunday = new Date(today);
      nextSunday.setDate(today.getDate() + daysUntilSun);
      var sundayAnchor = Utilities.formatDate(nextSunday, Session.getScriptTimeZone(), "yyyy-MM-dd");
      // Delivery date = Monday after Sunday
      var deliveryDate = new Date(nextSunday);
      deliveryDate.setDate(deliveryDate.getDate() + 1);
      var deliveryStr = Utilities.formatDate(deliveryDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
      // Get or create order ID
      var orderId = getNextOrderId(ssHub);
      // Get CorporateOrders sheet
      var corpSheet = ssHub.getSheetByName("CorporateOrders");
      if (!corpSheet) {
        corpSheet = ssHub.insertSheet("CorporateOrders");
        corpSheet.appendRow(["Timestamp","CompanyID","CompanyName","DeliveryDate","SundayAnchor","EmployeeName","EmployeeEmail","MealID","DishName","DietType","Tier","EmployeePrice","CompanyCoverage","BDCoverage","PaymentTransactionID","Status","OrderID","EmployeeLevel"]);
      }
      var totalItems = 0;
      Object.keys(levels).forEach(function(catId) {
        var lv = levels[catId];
        if (!lv || lv.status === 'paused' || !lv.qty || lv.qty <= 0) return;
        // One row per category with total qty
        corpSheet.appendRow([
          new Date(),
          companyId,
          companyName,
          deliveryStr,
          sundayAnchor,
          "Office Order (" + changedBy + ")",
          changedBy,
          "PAR-" + catId,
          catId + " x" + lv.qty,
          "par_level",
          "office",
          "0.00",
          "0.00",
          "0.00",
          "",
          "confirmed",
          orderId,
          "par_level"
        ]);
        totalItems += parseInt(lv.qty) || 0;
      });
      return jsonOut({success: true, orderId: orderId, totalItems: totalItems, deliveryDate: deliveryStr});
    }

    return ContentService.createTextOutput("Error: Unknown Action");
  } catch (err) {
    return ContentService.createTextOutput("Error: " + err.toString());
  }
}
// ══════════════════════════════════════════
// BENEFIT LEVELS HELPER
// ══════════════════════════════════════════
function _getBenefitLevels(ssHub, companyId) {
  var sheet = ssHub.getSheetByName("BenefitLevels");
  if (!sheet) return [];
  var rows = sheet.getDataRange().getValues();
  var headers = rows[0];
  var levels = [];
  for (var i = 1; i < rows.length; i++) {
    if (String(rows[i][0]).trim().toUpperCase() !== companyId) continue;
    var level = {};
    headers.forEach(function(h, idx) { level[h] = rows[i][idx]; });
    levels.push(level);
  }
  levels.sort(function(a, b) { return (a.LevelOrder || 0) - (b.LevelOrder || 0); });
  return levels;
}

function _getEmployeeBenefitLevel(ssHub, companyId, email) {
  var empSheet = ssHub.getSheetByName("Employees");
  if (!empSheet) return "General";
  var rows = empSheet.getDataRange().getValues();
  var headers = rows[0];
  var emailIdx = headers.indexOf("Email");
  var coIdx = headers.indexOf("CompanyID");
  var levelIdx = headers.indexOf("BenefitLevel");
  if (emailIdx < 0 || coIdx < 0 || levelIdx < 0) return "General";
  for (var i = 1; i < rows.length; i++) {
    if (String(rows[i][emailIdx]).trim().toLowerCase() === email.toLowerCase() &&
        String(rows[i][coIdx]).trim().toUpperCase() === companyId.toUpperCase()) {
      return String(rows[i][levelIdx] || "General").trim();
    }
  }
  return "General";
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

// ─────────────────────────────────────────────────────────────────────────────
// INVOICE SHEET HELPER
// ─────────────────────────────────────────────────────────────────────────────
function getOrCreateInvoiceSheet(ssHub) {
  var sheet = ssHub.getSheetByName("CompanyInvoices");
  if (!sheet) {
    sheet = ssHub.insertSheet("CompanyInvoices");
    sheet.appendRow([
      "InvoiceNumber","InvoiceID","CompanyID","CompanyName","BillingCycle",
      "PeriodStart","PeriodEnd","SundayAnchors","TotalOrders","TotalMeals",
      "TotalEmployees","SubtotalFullRetail","EmployeePaid","CompanyOwed",
      "BDContributed","CreditApplied","AmountDue","TierBreakdownJSON",
      "Status","CollectionMethod","DueDate","CreatedAt","SentAt","PaidAt",
      "PaidAmount","PaymentReference","PaymentMethod","OverdueReminderSent","Notes"
    ]);
    sheet.setFrozenRows(1);
    sheet.getRange(1,1,1,29).setFontWeight("bold").setBackground("#00465e").setFontColor("#ffffff");
  }
  return sheet;
}

// ─────────────────────────────────────────────────────────────────────────────
// INVOICE ROW READER  (header-based — never hardcode column indices)
// ─────────────────────────────────────────────────────────────────────────────
function _readInvoiceRow(headers, row) {
  var tz = Session.getScriptTimeZone();
  var obj = {};
  var dateFields = ["PeriodStart","PeriodEnd","DueDate","CreatedAt","SentAt","PaidAt"];
  for (var c = 0; c < headers.length; c++) {
    var h = headers[c];
    var val = row[c];
    if (dateFields.indexOf(h) >= 0 && val) {
      try { val = Utilities.formatDate(new Date(val), tz, "yyyy-MM-dd"); } catch(e) { val = String(val); }
    } else if (h === "TierBreakdownJSON" && val) {
      try { val = JSON.parse(val); } catch(e) { val = []; }
    }
    obj[h] = (val === undefined || val === null) ? "" : val;
  }
  return obj;
}

// ─────────────────────────────────────────────────────────────────────────────
// CORE INVOICE BUILDER  (shared by auto-Thursday trigger + manual generate)
// ─────────────────────────────────────────────────────────────────────────────
function _buildInvoiceForCompany(ssHub, companyId, sundayAnchor, skipIfExists) {
  var tz = Session.getScriptTimeZone();
  var invSheet  = getOrCreateInvoiceSheet(ssHub);
  var invRows   = invSheet.getDataRange().getValues();
  var invHeaders = invRows[0];

  // ── Look up company record (header-based) ──
  var compSheet = ssHub.getSheetByName("Companies");
  if (!compSheet) return {success:false, error:"Companies sheet not found"};
  var compRows = compSheet.getDataRange().getValues();
  var compHeaders = compRows[0];
  var compIdIdx         = compHeaders.indexOf("CompanyID");
  var compNameIdx       = compHeaders.indexOf("CompanyName");
  var billingCycleIdx   = compHeaders.indexOf("BillingCycle");
  var collectionIdx     = compHeaders.indexOf("CollectionMethod");
  var payTermsIdx       = compHeaders.indexOf("PaymentTerms");
  var invPrefixIdx      = compHeaders.indexOf("InvoicePrefix");
  var nextInvNumIdx     = compHeaders.indexOf("NextInvoiceNumber");
  var creditBalIdx      = compHeaders.indexOf("CreditBalance");
  var fullPriceIdx      = compHeaders.indexOf("FullPrice");

  var companyRow = -1, companyName = "", billingCycle = "weekly", collectionMethod = "";
  var paymentTerms = "due-on-receipt", invoicePrefix = "BD", nextInvoiceNumber = 1;
  var creditBalance = 0, fullPrice = 0;
  for (var k = 1; k < compRows.length; k++) {
    if (String(compRows[k][compIdIdx]).trim().toUpperCase() === companyId) {
      companyRow = k;
      companyName       = compNameIdx >= 0 ? String(compRows[k][compNameIdx] || "") : "";
      billingCycle      = billingCycleIdx >= 0 ? String(compRows[k][billingCycleIdx] || "weekly").trim() : "weekly";
      collectionMethod  = collectionIdx >= 0 ? String(compRows[k][collectionIdx] || "").trim() : "";
      paymentTerms      = payTermsIdx >= 0 ? String(compRows[k][payTermsIdx] || "due-on-receipt").trim() : "due-on-receipt";
      invoicePrefix     = invPrefixIdx >= 0 ? String(compRows[k][invPrefixIdx] || "BD").trim() : "BD";
      nextInvoiceNumber = nextInvNumIdx >= 0 ? (parseInt(compRows[k][nextInvNumIdx]) || 1) : 1;
      creditBalance     = creditBalIdx >= 0 ? (parseFloat(compRows[k][creditBalIdx]) || 0) : 0;
      fullPrice         = fullPriceIdx >= 0 ? (parseFloat(compRows[k][fullPriceIdx]) || 0) : 0;
      break;
    }
  }
  if (companyRow < 0) return {success:false, error:"Company not found: " + companyId};

  // ── Build InvoiceID (deterministic, for duplicate check) ──
  var invoiceId = "BD-" + sundayAnchor.replace(/-/g,"").slice(0,8) + "-" + companyId;

  // ── Duplicate check using InvoiceID column ──
  var invIdIdx = invHeaders.indexOf("InvoiceID");
  if (skipIfExists && invIdIdx >= 0) {
    for (var x = 1; x < invRows.length; x++) {
      if (String(invRows[x][invIdIdx]).trim() === invoiceId) return {success:false, skipped:true};
    }
  }

  // ── Pull orders for this company + week ──
  var corpSheet = ssHub.getSheetByName("CorporateOrders");
  if (!corpSheet) return {success:false, error:"No CorporateOrders sheet"};
  var rows    = corpSheet.getDataRange().getValues();
  var headers = rows[0];
  var coIdx   = headers.indexOf("CompanyID");
  var anIdx   = headers.indexOf("SundayAnchor");
  var epIdx   = headers.indexOf("EmployeePrice");
  var ccIdx   = headers.indexOf("CompanyCoverage");
  var bdIdx   = headers.indexOf("BDCoverage");
  var tiIdx   = headers.indexOf("Tier");
  var emIdx   = headers.indexOf("EmployeeEmail");
  var oidIdx  = headers.indexOf("OrderID");
  var cnIdx   = headers.indexOf("CompanyName");

  var empPaid = 0, compOwed = 0, bdContr = 0;
  var meals = 0, employees = {}, orders = {};
  var tierCounts = {Free:0, Tier1:0, Tier2:0, Tier3:0, Additional:0};
  var tierCompany = {Free:0, Tier1:0, Tier2:0, Tier3:0, Additional:0};
  var sundayAnchors = {};

  for (var i = 1; i < rows.length; i++) {
    if (!rows[i][0]) continue;
    if (String(rows[i][coIdx]).trim().toUpperCase() !== companyId) continue;
    var rawAnchor = rows[i][anIdx];
    var rowAnchor = rawAnchor instanceof Date
      ? Utilities.formatDate(rawAnchor, tz, "yyyy-MM-dd")
      : String(rawAnchor).trim();
    if (rowAnchor !== sundayAnchor) continue;
    meals++;
    empPaid  += parseFloat(rows[i][epIdx]) || 0;
    compOwed += parseFloat(rows[i][ccIdx]) || 0;
    bdContr  += parseFloat(rows[i][bdIdx]) || 0;
    employees[String(rows[i][emIdx]).trim().toLowerCase()] = true;
    if (oidIdx >= 0) orders[String(rows[i][oidIdx]).trim()] = true;
    if (!companyName && cnIdx >= 0) companyName = String(rows[i][cnIdx]).trim();
    sundayAnchors[rowAnchor] = true;
    var tier = String(rows[i][tiIdx] || "").trim();
    if (tierCounts[tier] !== undefined) {
      tierCounts[tier]++;
      tierCompany[tier] += parseFloat(rows[i][ccIdx]) || 0;
    }
  }

  if (meals === 0) return {success:false, error:"No orders found for this week"};

  // ── Generate sequential InvoiceNumber ──
  var paddedNum = String(nextInvoiceNumber);
  while (paddedNum.length < 3) paddedNum = "0" + paddedNum;
  var invoiceNumber = invoicePrefix + "-" + paddedNum;

  // Increment NextInvoiceNumber on the Companies sheet
  if (nextInvNumIdx >= 0 && companyRow >= 0) {
    compSheet.getRange(companyRow + 1, nextInvNumIdx + 1).setValue(nextInvoiceNumber + 1);
  }

  // ── Calculate PeriodStart / PeriodEnd from sundayAnchor ──
  var anchorDate = new Date(sundayAnchor + "T12:00:00");
  var periodStart = new Date(anchorDate);
  periodStart.setDate(anchorDate.getDate() + 1); // Monday
  var periodEnd = new Date(anchorDate);
  periodEnd.setDate(anchorDate.getDate() + 5); // Friday

  // ── Calculate DueDate from PaymentTerms ──
  var dueDate = new Date(periodStart);
  if (paymentTerms === "net-15") {
    dueDate.setDate(periodStart.getDate() + 15);
  } else if (paymentTerms === "net-30") {
    dueDate.setDate(periodStart.getDate() + 30);
  } else if (paymentTerms === "net-45") {
    dueDate.setDate(periodStart.getDate() + 45);
  } else if (paymentTerms === "net-60") {
    dueDate.setDate(periodStart.getDate() + 60);
  } else if (paymentTerms === "net-7") {
    dueDate.setDate(periodStart.getDate() + 7);
  }
  // "due-on-receipt" → dueDate stays as periodStart

  // ── SubtotalFullRetail ──
  var subtotalFullRetail = Math.round(meals * fullPrice * 100) / 100;

  // ── Apply CreditBalance ──
  compOwed = Math.round(compOwed * 100) / 100;
  var creditApplied = 0;
  var amountDue = compOwed;
  if (creditBalance > 0) {
    creditApplied = Math.min(creditBalance, compOwed);
    amountDue = Math.round((compOwed - creditApplied) * 100) / 100;
    // Update company's CreditBalance
    if (creditBalIdx >= 0 && companyRow >= 0) {
      compSheet.getRange(companyRow + 1, creditBalIdx + 1).setValue(Math.round((creditBalance - creditApplied) * 100) / 100);
    }
  }
  creditApplied = Math.round(creditApplied * 100) / 100;

  // ── Build tier breakdown JSON ──
  var breakdown = [];
  var tierLabels = {Free:"Free meals", Tier1:"Tier 1", Tier2:"Tier 2", Tier3:"Tier 3", Additional:"Additional"};
  Object.keys(tierCounts).forEach(function(t) {
    if (tierCounts[t] > 0) breakdown.push({tier:tierLabels[t], meals:tierCounts[t], companyTotal:Math.round(tierCompany[t]*100)/100});
  });

  // ── Write all 29 columns using header-based lookups ──
  var newRow = [];
  for (var c = 0; c < invHeaders.length; c++) {
    var h = invHeaders[c];
    switch(h) {
      case "InvoiceNumber":      newRow.push(invoiceNumber); break;
      case "InvoiceID":          newRow.push(invoiceId); break;
      case "CompanyID":          newRow.push(companyId); break;
      case "CompanyName":        newRow.push(companyName); break;
      case "BillingCycle":       newRow.push(billingCycle); break;
      case "PeriodStart":        newRow.push(periodStart); break;
      case "PeriodEnd":          newRow.push(periodEnd); break;
      case "SundayAnchors":      newRow.push(Object.keys(sundayAnchors).join(",")); break;
      case "TotalOrders":        newRow.push(Object.keys(orders).length || meals); break;
      case "TotalMeals":         newRow.push(meals); break;
      case "TotalEmployees":     newRow.push(Object.keys(employees).length); break;
      case "SubtotalFullRetail": newRow.push(subtotalFullRetail); break;
      case "EmployeePaid":       newRow.push(Math.round(empPaid*100)/100); break;
      case "CompanyOwed":        newRow.push(compOwed); break;
      case "BDContributed":      newRow.push(Math.round(bdContr*100)/100); break;
      case "CreditApplied":      newRow.push(creditApplied); break;
      case "AmountDue":          newRow.push(amountDue); break;
      case "TierBreakdownJSON":  newRow.push(JSON.stringify(breakdown)); break;
      case "Status":             newRow.push("pending"); break;
      case "CollectionMethod":   newRow.push(collectionMethod); break;
      case "DueDate":            newRow.push(dueDate); break;
      case "CreatedAt":          newRow.push(new Date()); break;
      case "SentAt":             newRow.push(""); break;
      case "PaidAt":             newRow.push(""); break;
      case "PaidAmount":         newRow.push(""); break;
      case "PaymentReference":   newRow.push(""); break;
      case "PaymentMethod":      newRow.push(""); break;
      case "OverdueReminderSent": newRow.push(""); break;
      case "Notes":              newRow.push(""); break;
      default:                   newRow.push(""); break;
    }
  }
  invSheet.appendRow(newRow);
  return {success:true, invoiceId:invoiceId, invoiceNumber:invoiceNumber};
}

// ─────────────────────────────────────────────────────────────────────────────
// THURSDAY AUTO-TRIGGER  — set this up in GAS:
//   Triggers → Add trigger → generateWeeklyInvoices
//   Time-based → Week timer → Every Thursday → 8am–9am
// ─────────────────────────────────────────────────────────────────────────────
function generateWeeklyInvoices() {
  var ssHub = SpreadsheetApp.getActiveSpreadsheet();

  // Find the Sunday anchor for the week that just closed
  // Delivery was Monday, orders closed Wednesday — we look back to last Monday's Sunday anchor
  var today     = new Date();
  var dayOfWeek = today.getDay(); // 0=Sun, 4=Thu
  var daysBack  = dayOfWeek === 4 ? 3 : dayOfWeek + 4; // land on last Monday
  var lastMonday = new Date(today); lastMonday.setDate(today.getDate() - daysBack);
  // Sunday anchor = Sunday before that Monday
  var sun = new Date(lastMonday); sun.setDate(lastMonday.getDate() - 1);
  var sundayAnchor = Utilities.formatDate(sun, Session.getScriptTimeZone(), "yyyy-MM-dd");

  // Get all unique CompanyIDs from CorporateOrders for this week
  var corpSheet = ssHub.getSheetByName("CorporateOrders");
  if (!corpSheet) return;
  var rows    = corpSheet.getDataRange().getValues();
  var headers = rows[0];
  var coIdx   = headers.indexOf("CompanyID");
  var anIdx   = headers.indexOf("SundayAnchor");
  var companies = {};
  for (var i = 1; i < rows.length; i++) {
    var rawAn = rows[i][anIdx];
    var rowAn = rawAn instanceof Date
      ? Utilities.formatDate(rawAn, Session.getScriptTimeZone(), "yyyy-MM-dd")
      : String(rawAn).trim();
    if (rowAn === sundayAnchor && rows[i][coIdx]) {
      companies[String(rows[i][coIdx]).trim().toUpperCase()] = true;
    }
  }

  var created = 0, skipped = 0, errors = [];
  Object.keys(companies).forEach(function(companyId) {
    var result = _buildInvoiceForCompany(ssHub, companyId, sundayAnchor, true);
    if (result.success) {
      created++;
      Logger.log("  Created invoice " + (result.invoiceNumber || result.invoiceId) + " for " + companyId);
    } else if (result.skipped) {
      skipped++;
    } else {
      errors.push(companyId + ": " + (result.error || "unknown"));
    }
  });
  Logger.log("generateWeeklyInvoices: anchor=" + sundayAnchor + " created=" + created + " skipped=" + skipped + " errors=" + errors.length);
  if (errors.length > 0) Logger.log("  Errors: " + errors.join("; "));
}
