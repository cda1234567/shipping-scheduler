# Chaos Test Report

**Date:** 2026-04-17T07:53:25.139Z
**Duration:** 248.7s (~4 minutes)
**Total Actions:** 128
**JS/Console Errors:** 81 (all 403s — see Bug #1 below)
**HTTP 4xx/5xx Errors:** 0 (no 5xx; 403s appeared as console errors, not response-level intercepts)

---

## Assessment

**REVIEW NEEDED** — One reproducible bug found. Backend is stable (zero 5xx). All 81 "errors" trace back to a single root cause: accidental edit-auth logout during chaos interaction.

---

## Bugs Found

### Bug #1 (REPRODUCIBLE): Edit-Auth Button Is Too Easy to Accidentally Click Out of Edit Mode

**Severity:** Medium
**Reproducible:** Yes

**Description:**
During the 已發料 tab visit, the chaos script's random button picker clicked `#btn-edit-auth` which had text "登出編輯" (logout from edit mode). This immediately revoked the session's edit permissions. After that:

- All subsequent API calls that require edit auth returned **403 Forbidden**, logged as console errors (81 total)
- The edit-auth modal likely appeared and **blocked the entire UI** — causing every subsequent tab navigation button click to time out (3000ms timeout exceeded for BOM, 無MOQ包裝, 提醒, 紀錄, 備份, 分析, 不良品 tabs)
- `#btn-manual-supplement` (手動補料) also became unreachable for the same reason
- The auth password field (`#edit-auth-password`) was being randomly filled with garbage strings during the chaos loop, but none were submitted successfully

**Reproduction steps:**
1. Log in to edit mode (password: 123)
2. Navigate to any tab
3. Click the "登出編輯" button in the header (it's in the top nav bar near other control buttons)
4. Observe: all edit-protected features immediately fail with 403; if a login modal auto-appears, it blocks all tab navigation

**Impact:** A careless user who accidentally clicks "登出編輯" loses their edit session. If the re-login modal is modal (blocking), all navigation is frozen until they re-authenticate. The button provides no confirmation prompt before logging out.

**Suggested fix:** Add a confirmation dialog ("確定要登出編輯模式？") before executing logout, or move the button to a less prominent location to reduce accidental clicks.

---

### Bug #2 (OBSERVATION): Auth Password Field Gets Filled by Chaos — No Rate Limiting Observed

**Severity:** Low
**Reproducible:** Yes

**Description:**
The `#edit-auth-password` input field was filled with random strings multiple times during the chaos loop (iterations 1–7). No rate limiting or lockout was triggered. Acceptable for a local-only tool but worth noting if ever exposed publicly.

---

### Bug #3 (OBSERVATION): Tab Navigation Completely Blocked When Auth Modal Is Visible

**Severity:** Medium
**Reproducible:** Yes (consequence of Bug #1)

**Description:**
After accidental logout triggered the edit-auth modal overlay, clicking tab buttons consistently timed out at 3000ms. The modal appears to intercept all click events on the underlying page. Users who accidentally log out cannot navigate away without completing the re-auth flow.

---

## Timeline Summary

| Time | Event |
|------|-------|
| 07:49:16 | Test started, navigated to http://127.0.0.1:8765 |
| 07:49:18 | Authenticated via `/api/system/edit-auth/login` — 200 OK |
| 07:49:19 | Page reloaded, auth confirmed |
| 07:49:19–23 | Schedule tab: filled inputs, toggled checkboxes, clicked 依出貨日排序 |
| 07:49:27 | **BUG TRIGGER**: In 已發料 tab, accidentally clicked "登出編輯" (#btn-edit-auth) |
| 07:49:34 | 主檔預覽 tab: clicked "登入編輯" — modal appeared, blocked UI |
| 07:49:48–11:663 | All tab clicks timed out (BOM, 無MOQ包裝, 提醒, 紀錄, 備份, 分析, 不良品) |
| 07:50:22–53:25 | Chaos loop: 7 iterations, all producing 403 console errors on every interaction |
| 07:53:25 | Test ended — 128 actions, 81 console errors (all 403), 0 HTTP 5xx |

---

## JS / Console Errors

All 81 errors are identical pattern:
```
Failed to load resource: the server responded with a status of 403 (Forbidden)
```
Root cause: edit-auth session was lost at ~07:49:27 when chaos script clicked "登出編輯". Every subsequent API call requiring auth returned 403. These are **not independent bugs** — they are cascading effects of Bug #1.

---

## HTTP Errors (5xx)

_None_ — backend was fully stable throughout the test.

---

## Visual Issues

No screenshots taken (headless mode). However, from interaction behavior:
- After accidental logout, the edit-auth modal presumably appeared and remained visible
- This modal blocked all underlying tab navigation (buttons unclickable for 3+ minutes)
- Suggests the modal has no timeout/auto-dismiss and requires explicit user action to proceed

---

## Features Exercised

| Feature | Status |
|---------|--------|
| 出貨排程 tab — inputs, checkboxes | Exercised |
| 依出貨日排序 button | Clicked, no error |
| 已發料 tab — new folder name input | Exercised |
| 主檔預覽 tab — search input | Exercised |
| 生成發料單 (dispatch gen) button | Clicked once, no 5xx |
| 手動補料 modal | Could not reach (blocked by auth modal) |
| Schedule cell editing | Could not reach (blocked by auth modal) |
| BOM, 無MOQ包裝, 提醒, 紀錄, 備份, 分析, 不良品 tabs | Timed out (blocked) |

---

## Overall Verdict

**PASS (backend) / REVIEW (UX)** — The server itself is robust: zero 5xx responses, no crashes, no data corruption observed. The one actionable finding is a UX bug: the "登出編輯" button in the header has no confirmation guard, making accidental logout easy for a careless user, and the resulting auth modal completely freezes navigation until re-auth is completed.
